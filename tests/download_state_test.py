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
        # Tables exist.
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"collection_entries", "collection_metadata"} <= names
        # Exactly one metadata row — mark_listing_complete()'s no-WHERE UPDATE
        # relies on this singleton invariant.
        count = conn.execute("SELECT COUNT(*) FROM collection_metadata").fetchone()[0]
        assert count == 1
        # metadata row was seeded with NULL listing_completed_at.
        row = conn.execute(
            "SELECT listing_completed_at FROM collection_metadata"
        ).fetchone()
        assert row == (None,)


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

    db.mark_downloaded("ds1", "s3://b/k")

    entries = list(db.iter_entries_for_dataset("ds1"))
    assert entries[0].downloaded is True


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


def test_is_listing_fresh_false_when_missing(tmp_path: Path) -> None:
    db = DownloadStateDB.for_collection(tmp_path, "never-initialized")
    assert db.is_listing_fresh() is False


def test_is_listing_fresh_false_when_listing_in_progress(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    # init_fresh leaves listing_completed_at = NULL; that's "listing in progress".
    assert db.is_listing_fresh() is False


def test_is_listing_fresh_true_after_mark_complete(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.mark_listing_complete()
    assert db.is_listing_fresh() is True


def test_is_listing_fresh_false_after_ttl_expiry(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    # Hand-write a timestamp older than TTL — simpler than freezing time.
    expired = (
        datetime.now(timezone.utc) - download_state.LISTING_TTL - timedelta(minutes=1)
    )
    with sqlite3.connect(db.path) as conn:
        conn.execute(
            "UPDATE collection_metadata SET listing_completed_at = ?",
            (expired.isoformat(),),
        )
        conn.commit()

    assert db.is_listing_fresh() is False


def test_is_listing_fresh_false_for_corrupted_db(tmp_path: Path) -> None:
    db = DownloadStateDB.for_collection(tmp_path, "corrupt")
    db.path.parent.mkdir(parents=True, exist_ok=True)
    db.path.write_bytes(b"this is not a sqlite database")

    # Should return False (treat as not fresh → trigger fresh re-init) rather
    # than raising into the orchestrator.
    assert db.is_listing_fresh() is False


def test_delete_removes_db_and_wal_sidecars(tmp_path: Path) -> None:
    db = _new_db(tmp_path)
    db.insert_entries([CollectionEntry("ds1", "s3://b/k", 100, downloaded=False)])
    # Force WAL/SHM sidecars to materialize by holding a write. Close the
    # connection before delete() — sqlite3's `with` commits but does NOT close,
    # and unlinking a file with an open handle fails on Windows.
    conn = sqlite3.connect(db.path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "INSERT INTO collection_entries (dataset_slug, url, expected_size, downloaded) "
            "VALUES ('ds2', 's3://b/z', 1, 0)"
        )
        conn.commit()
    finally:
        conn.close()

    db.delete()

    for suffix in ("", "-wal", "-shm"):
        assert not Path(str(db.path) + suffix).exists()


def test_delete_idempotent_when_db_missing(tmp_path: Path) -> None:
    db = DownloadStateDB.for_collection(tmp_path, "ghost")
    # Doesn't raise even when nothing's there.
    db.delete()
