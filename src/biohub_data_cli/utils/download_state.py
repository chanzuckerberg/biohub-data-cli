"""SQLite-backed resume state for `download collection`.

One DB per collection, at `{outdir}/{collection_slug}/.biohub-data-cli/state.db`.
Holds the cached S3/HTTP listing plus a per-file `downloaded` flag, so an
interrupted run can pick up where it left off without re-listing.

Listings are cached for `LISTING_TTL` (5 days) — bucket contents change, so
stale entries get nuked rather than blindly trusted.

Concurrency: none. All DB access happens on the main thread.
"""

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

LISTING_TTL = timedelta(days=5)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS collection_entries (
    dataset_slug  TEXT NOT NULL,
    url           TEXT NOT NULL,
    expected_size INTEGER,
    downloaded    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (dataset_slug, url)
);
CREATE TABLE IF NOT EXISTS collection_metadata (
    listing_completed_at TEXT
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
        """Remove any existing DB at this path and recreate empty tables.

        Used at the start of a non-resumed run, and after detecting a stale
        or incomplete listing. After this call, `listing_completed_at` is NULL
        — the caller must call `mark_listing_complete()` once the listing
        loop finishes successfully.
        """
        self._unlink()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT INTO collection_metadata (listing_completed_at) VALUES (NULL)"
            )
            conn.commit()

    # ── listing freshness ──────────────────────────────────────────────

    def is_listing_fresh(self) -> bool:
        """True iff the DB has a non-NULL `listing_completed_at` within TTL.

        Returns False for missing DB, corrupted DB, never-completed listing,
        or expired listing — all four collapse into "don't trust, re-list".
        """
        if not self.path.exists():
            return False
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT listing_completed_at FROM collection_metadata LIMIT 1"
                ).fetchone()
            if row is None or row[0] is None:
                return False
            completed_at = datetime.fromisoformat(row[0])
            return datetime.now(timezone.utc) - completed_at < LISTING_TTL
        except (sqlite3.DatabaseError, ValueError):
            return False

    def mark_listing_complete(self) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE collection_metadata SET listing_completed_at = ?",
                (datetime.now(timezone.utc).isoformat(),),
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
