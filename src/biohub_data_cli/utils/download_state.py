"""SQLite-backed resume state for `download collection`.

One DB per collection, at `{outdir}/{collection_slug}/.biohub-data-cli/state.db`.
Holds the cached S3/HTTP listing plus a per-file `downloaded` flag, so an
interrupted run can pick up where it left off without re-listing.

Listing freshness is tracked per dataset (`dataset_listings.listed_at`): a
dataset's cached listing is reused within `LISTING_TTL` and re-listed after.

Concurrency: none. All DB access happens on the main thread.
"""

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

LISTING_TTL = timedelta(days=5)

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS collection_entries (
    dataset_slug  TEXT NOT NULL,
    url           TEXT NOT NULL,
    expected_size INTEGER,
    downloaded    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (dataset_slug, url)
);
CREATE TABLE IF NOT EXISTS dataset_listings (
    dataset_slug TEXT PRIMARY KEY,
    listed_at    TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class CollectionEntry:
    dataset_slug: str
    url: str
    expected_size: int | None
    downloaded: bool


@dataclass(frozen=True)
class DatasetProgress:
    """Aggregate counts/bytes for one dataset, computed in a single SQL pass."""

    total_count: int
    pending_count: int
    total_bytes: int
    done_bytes: int


class DownloadStateDB:
    """Per-collection resume state."""

    def __init__(self, path: Path) -> None:
        self.path = path

    @classmethod
    def for_collection(cls, outdir: Path, collection_slug: str) -> "DownloadStateDB":
        return cls(outdir / collection_slug / ".biohub-data-cli" / "state.db")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        try:
            # 5s busy timeout — give SQLite room to retry instead of failing
            # fast if it ever does encounter a writer collision.
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
        finally:
            conn.close()

    # ── lifecycle ──────────────────────────────────────────────────────

    def _unlink(self) -> None:
        self.path.unlink(missing_ok=True)

    def exists(self) -> bool:
        return self.path.exists()

    def init_fresh(self) -> None:
        """Remove any existing DB and recreate empty tables stamped with the
        current schema version. Used for `--no-resume` and as the rebuild path
        when an existing DB is absent, corrupt, or a different schema version.
        """
        self._unlink()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.executescript(_SCHEMA)
            conn.commit()

    def ensure_ready(self) -> None:
        """Make the DB usable for a resumed run, preserving a valid same-version
        DB so resume works. Creates it if absent, and discards-and-rebuilds if
        it's corrupt or written by a different schema version.
        """
        if not self.path.exists():
            self.init_fresh()
            return
        try:
            with self._connect() as conn:
                version = conn.execute("PRAGMA user_version").fetchone()[0]
        except sqlite3.DatabaseError:
            version = None
        if version != SCHEMA_VERSION:
            self.init_fresh()

    # ── listing freshness (per dataset) ─────────────────────────────────

    def get_unexpired_dataset_slugs(self) -> set[str]:
        """Datasets whose cached listing is still within TTL and can be reused
        without re-listing. Stale or malformed rows are omitted (→ re-list).
        """
        cutoff = datetime.now(timezone.utc) - LISTING_TTL
        unexpired: set[str] = set()
        with self._connect() as conn:
            for slug, listed_at in conn.execute(
                "SELECT dataset_slug, listed_at FROM dataset_listings"
            ):
                try:
                    if datetime.fromisoformat(listed_at) > cutoff:
                        unexpired.add(slug)
                except ValueError:
                    continue
        return unexpired

    def mark_dataset_listed(self, dataset_slug: str) -> None:
        """Record that `dataset_slug` was just listed cleanly."""
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO dataset_listings (dataset_slug, listed_at) VALUES (?, ?) "
                "ON CONFLICT(dataset_slug) DO UPDATE SET listed_at = excluded.listed_at",
                (dataset_slug, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()

    def delete_dataset_entries(self, dataset_slug: str) -> None:
        """Drop a dataset's entries and listing record before a re-list, so a
        stale or partial listing doesn't leave orphaned rows.
        """
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM collection_entries WHERE dataset_slug = ?", (dataset_slug,)
            )
            conn.execute(
                "DELETE FROM dataset_listings WHERE dataset_slug = ?", (dataset_slug,)
            )
            conn.commit()

    # ── entries ────────────────────────────────────────────────────────

    def insert_entries(self, entries: list[CollectionEntry]) -> None:
        """Bulk-insert entries during the listing phase. `INSERT OR IGNORE`
        makes re-listing the same dataset a no-op rather than a primary-key
        collision — useful if a partial listing crashes and the next run
        re-walks the same prefix.
        """
        if not entries:
            return
        with self._connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO collection_entries "
                "(dataset_slug, url, expected_size, downloaded) VALUES (?, ?, ?, ?)",
                [
                    (e.dataset_slug, e.url, e.expected_size, int(e.downloaded))
                    for e in entries
                ],
            )
            conn.commit()

    def mark_downloaded(self, dataset_slug: str, url: str, size: int) -> None:
        """Flag a (dataset, url) as downloaded and record its byte size."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE collection_entries SET downloaded = 1, expected_size = ? "
                "WHERE dataset_slug = ? AND url = ?",
                (size, dataset_slug, url),
            )
            conn.commit()

    def dataset_progress(self, dataset_slug: str) -> DatasetProgress:
        """Aggregate one dataset's progress."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT "
                "COUNT(*), "
                "COALESCE(SUM(CASE WHEN downloaded = 0 THEN 1 ELSE 0 END), 0), "
                "COALESCE(SUM(expected_size), 0), "
                "COALESCE(SUM(CASE WHEN downloaded = 1 THEN expected_size END), 0) "
                "FROM collection_entries WHERE dataset_slug = ?",
                (dataset_slug,),
            ).fetchone()
        return DatasetProgress(
            total_count=row[0],
            pending_count=row[1],
            total_bytes=row[2],
            done_bytes=row[3],
        )

    def iter_entries_for_dataset(
        self, dataset_slug: str, *, pending_only: bool = False
    ) -> Iterator[CollectionEntry]:
        """Stream `CollectionEntry` rows for one dataset via cursor iteration.

        The connection is held open until the iterator is exhausted (the
        `with` block exits on generator return). Consumers should iterate to
        completion or wrap calls in `list(...)`; partial iteration leaves the
        connection alive until GC reclaims the generator.
        """
        query = (
            "SELECT dataset_slug, url, expected_size, downloaded "
            "FROM collection_entries WHERE dataset_slug = ?"
        )
        if pending_only:
            query += " AND downloaded = 0"
        with self._connect() as conn:
            for r in conn.execute(query, (dataset_slug,)):
                yield CollectionEntry(
                    dataset_slug=r[0],
                    url=r[1],
                    expected_size=r[2],
                    downloaded=bool(r[3]),
                )
