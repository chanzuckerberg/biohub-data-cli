import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from biohub_data_cli.utils import download_state
from biohub_data_cli.utils.download_state import CollectionEntry, DownloadStateDB


def _new_db(tmp_path: Path) -> DownloadStateDB:
    db = DownloadStateDB.for_collection(tmp_path, "test-coll")
    db.init_fresh()
    return db


def test_for_collection_uses_conventional_path(tmp_path: Path) -> None:
    db = DownloadStateDB.for_collection(tmp_path, "my-coll")
    assert db.path == tmp_path / "my-coll" / ".biohub-data-cli" / "state.db"


def test_init_fresh_creates_parent_dirs_and_empty_tables(tmp_path: Path) -> None:
    db = _new_db(tmp_path)

    assert db.path.exists()
    with sqlite3.connect(db.path) as conn:
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"collection_entries", "dataset_listings"} <= names
        # Stamped with the current schema version for cross-version detection.
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == download_state.SCHEMA_VERSION


def test_init_fresh_nukes_existing_state(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries([CollectionEntry("ds1", "s3://b/k", 100, downloaded=True)])

    db.init_fresh()
    assert list(db.iter_entries_for_dataset("ds1")) == []


def test_insert_entries_idempotent_on_pk_collision(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    entry = CollectionEntry("ds1", "s3://b/k", 100, downloaded=False)

    db.insert_entries([entry])
    # Re-inserting with downloaded=True should be ignored — `downloaded` flag
    # is owned by mark_downloaded(), not insert_entries().
    db.insert_entries([CollectionEntry("ds1", "s3://b/k", 100, downloaded=True)])

    entries = list(db.iter_entries_for_dataset("ds1"))
    assert len(entries) == 1
    assert entries[0].downloaded is False


def test_mark_downloaded_flips_flag(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries([CollectionEntry("ds1", "s3://b/k", 100, downloaded=False)])

    db.mark_downloaded("ds1", "s3://b/k", size=100)

    entries = list(db.iter_entries_for_dataset("ds1"))
    assert entries[0].downloaded is True
    assert entries[0].expected_size == 100


def test_mark_downloaded_with_size_records_expected_size(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    # Listed with unknown size (NULL), as an HTTP file without Content-Length.
    db.insert_entries([CollectionEntry("ds1", "https://h/f", None, downloaded=False)])

    db.mark_downloaded("ds1", "https://h/f", size=4096)

    entry = next(iter(db.iter_entries_for_dataset("ds1")))
    assert entry.downloaded is True
    assert entry.expected_size == 4096
    # And it now contributes to the progress byte totals.
    assert db.dataset_progress("ds1").done_bytes == 4096


def test_iter_entries_for_dataset_is_scoped(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries(
        [
            CollectionEntry("ds1", "s3://b/a", 100, downloaded=False),
            CollectionEntry("ds1", "s3://b/b", 200, downloaded=False),
            CollectionEntry("ds2", "s3://b/c", 300, downloaded=False),
        ]
    )

    ds1 = list(db.iter_entries_for_dataset("ds1"))
    ds2 = list(db.iter_entries_for_dataset("ds2"))

    assert {e.url for e in ds1} == {"s3://b/a", "s3://b/b"}
    assert {e.url for e in ds2} == {"s3://b/c"}


def test_iter_entries_for_dataset_pending_only(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries(
        [
            CollectionEntry("ds1", "s3://b/a", 100, downloaded=False),
            CollectionEntry("ds1", "s3://b/b", 200, downloaded=False),
            CollectionEntry("ds1", "s3://b/c", 300, downloaded=False),
        ]
    )
    db.mark_downloaded("ds1", "s3://b/b", size=200)

    pending = list(db.iter_entries_for_dataset("ds1", pending_only=True))

    assert {e.url for e in pending} == {"s3://b/a", "s3://b/c"}
    assert all(e.downloaded is False for e in pending)


def test_dataset_progress_aggregates_counts_and_bytes(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries(
        [
            CollectionEntry("ds1", "s3://b/a", 100, downloaded=False),
            CollectionEntry("ds1", "s3://b/b", 200, downloaded=False),
            # NULL size must count as 0 bytes, matching the download path.
            CollectionEntry("ds1", "s3://b/c", None, downloaded=False),
            CollectionEntry("ds2", "s3://b/other", 999, downloaded=False),
        ]
    )
    db.mark_downloaded("ds1", "s3://b/b", size=200)

    p = db.dataset_progress("ds1")

    assert p.total_count == 3  # ds2 excluded — scoped to ds1
    assert p.pending_count == 2  # a and c still pending
    assert p.total_bytes == 300  # 100 + 200 + 0 (NULL)
    assert p.done_bytes == 200  # only b is downloaded


def test_dataset_progress_pending_unknown_size_counts_zero_bytes(
    tmp_path: Path,
) -> None:
    """A pending HTTP file listed with unknown (NULL) size counts toward the
    file totals but contributes 0 bytes — matching how the download path treats
    unknown sizes. (Once downloaded, mark_downloaded records its real size.)
    """
    db = _new_db(tmp_path)
    db.insert_entries(
        [
            CollectionEntry("ds1", "https://h/known", 100, downloaded=False),
            CollectionEntry("ds1", "https://h/unknown", None, downloaded=False),
        ]
    )

    p = db.dataset_progress("ds1")

    assert p.total_count == 2
    assert p.pending_count == 2
    assert p.total_bytes == 100  # unknown-size file contributes 0
    assert p.done_bytes == 0


def test_dataset_progress_empty_dataset(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    # No rows for this dataset → all zeros (lets the caller tell "empty" from
    # "fully downloaded", which is pending_count == 0 with total_count > 0).
    p = db.dataset_progress("nonexistent")
    assert (p.total_count, p.pending_count, p.total_bytes, p.done_bytes) == (0, 0, 0, 0)


def test_get_unexpired_dataset_slugs_empty_after_init(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    assert db.get_unexpired_dataset_slugs() == set()


def test_mark_dataset_listed_makes_slug_fresh(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.mark_dataset_listed("ds1")
    db.mark_dataset_listed("ds2")
    assert db.get_unexpired_dataset_slugs() == {"ds1", "ds2"}


def test_mark_dataset_listed_is_idempotent(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.mark_dataset_listed("ds1")
    db.mark_dataset_listed("ds1")  # ON CONFLICT updates listed_at, no PK error
    assert db.get_unexpired_dataset_slugs() == {"ds1"}


def test_get_unexpired_dataset_slugs_excludes_ttl_expired(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.mark_dataset_listed("fresh-ds")
    # Hand-write an expired timestamp — simpler than freezing time.
    expired = (
        datetime.now(timezone.utc) - download_state.LISTING_TTL - timedelta(minutes=1)
    )
    with sqlite3.connect(db.path) as conn:
        conn.execute(
            "INSERT INTO dataset_listings (dataset_slug, listed_at) VALUES (?, ?)",
            ("stale-ds", expired.isoformat()),
        )
        conn.commit()

    assert db.get_unexpired_dataset_slugs() == {"fresh-ds"}


def test_delete_dataset_entries_clears_rows_and_listing(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries(
        [
            CollectionEntry("ds1", "s3://b/a", 100, downloaded=False),
            CollectionEntry("ds2", "s3://b/c", 300, downloaded=False),
        ]
    )
    db.mark_dataset_listed("ds1")
    db.mark_dataset_listed("ds2")

    db.delete_dataset_entries("ds1")

    assert list(db.iter_entries_for_dataset("ds1")) == []
    assert db.get_unexpired_dataset_slugs() == {"ds2"}  # ds2 untouched
    assert {e.url for e in db.iter_entries_for_dataset("ds2")} == {"s3://b/c"}


def test_ensure_ready_creates_db_when_absent(tmp_path: Path) -> None:
    db = DownloadStateDB.for_collection(tmp_path, "new-coll")
    assert not db.exists()

    db.ensure_ready()

    assert db.exists()
    assert db.get_unexpired_dataset_slugs() == set()


def test_ensure_ready_preserves_same_version_db(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries([CollectionEntry("ds1", "s3://b/k", 100, downloaded=True)])
    db.mark_dataset_listed("ds1")

    db.ensure_ready()  # same SCHEMA_VERSION → must not nuke

    assert db.get_unexpired_dataset_slugs() == {"ds1"}
    assert len(list(db.iter_entries_for_dataset("ds1"))) == 1


def test_ensure_ready_rebuilds_on_schema_version_mismatch(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries([CollectionEntry("ds1", "s3://b/k", 100, downloaded=True)])
    db.mark_dataset_listed("ds1")
    # Simulate a DB written by a different schema version.
    with sqlite3.connect(db.path) as conn:
        conn.execute(f"PRAGMA user_version = {download_state.SCHEMA_VERSION + 1}")
        conn.commit()

    db.ensure_ready()

    # Discarded and rebuilt: prior state gone, version reset to current.
    assert list(db.iter_entries_for_dataset("ds1")) == []
    assert db.get_unexpired_dataset_slugs() == set()
    with sqlite3.connect(db.path) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == download_state.SCHEMA_VERSION


def test_ensure_ready_rebuilds_corrupted_db(tmp_path: Path) -> None:
    db = DownloadStateDB.for_collection(tmp_path, "corrupt")
    db.path.parent.mkdir(parents=True, exist_ok=True)
    db.path.write_bytes(b"this is not a sqlite database")

    db.ensure_ready()  # rebuild rather than raise into the orchestrator

    assert db.get_unexpired_dataset_slugs() == set()
