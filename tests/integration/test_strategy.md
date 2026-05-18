# Integration test strategy

These tests verify the **correctness of the download functionality end-to-end** against fixtures in `tests/integration/fixtures/`. They exercise the real networking stack — actual S3 API calls (`list_objects_v2`, `get_object`), real HTTP requests, real socket/TLS handshakes, real on-disk writes — none of which are covered by the mock-based unit tests in `tests/*_test.py`. The goal is to catch bugs that only surface against real transports: pagination, redirects, multipart, concurrency under real latency, and the various ways networking, S3 listing, and HTTP semantics can diverge from their mocked stand-ins.

Unit tests cover wiring and edge cases against mocks; this document covers the live-network / live-S3 layer.

## Status

| Tier | Status |
|---|---|
| Tier 1 — correctness | Implemented |
| Tier 2 — content integrity | Not implemented |
| Tier 3 — performance & observability | Not implemented |

## Tier 1 — correctness (cheap, must-have)

- **Key-set equality.** Compare `{s3 keys expanded by expand_s3_location}` against `{relative paths under outdir/<collection.slug>/<dataset.slug>/}`. A single set-diff catches both "missing file" and "extra file" at once. The expected set is free from `list_objects_v2`.
- **Per-file size.** Source size from `list_objects_v2` / `head_object` (S3) or `Content-Length` (HTTP) vs. `os.stat().st_size` locally. Catches truncation, partial-stream writes, and chunked-encoding bugs without extra I/O.
- **Failures list shape.** Happy path: `failures == []`. Negative paths: each `DownloadFailure` carries the correct `collection_slug` / `dataset_slug` / `url` and a non-empty `reason`.

## Tier 2 — content integrity (only if you don't trust transport)

TCP+TLS already protect against silent corruption, so checksums mostly catch *application* bugs: wrong chunk written to wrong file, race conditions, concurrent-writer overlap.

- **S3:** capture `ETag` from `list_objects_v2` during sourcing. Single-part uploads have ETag = MD5, comparable to `hashlib.md5(local_bytes).hexdigest()`. Multipart uploads (`ETag` ends in `-N`) need the multipart-MD5 reconstruction algorithm — easier to just skip those and fall back to size-only.
- **HTTP:** check `Content-MD5` header if present; otherwise size-only.
- **Cost:** re-reads every downloaded byte locally. Roughly 2x the I/O of the download itself. Don't run on the extra-large fixture.

## Tier 3 — performance & observability (regression signals, not pass/fail)

- **Wall-clock time.** Log it for trend-watching; do not assert as pass/fail (network jitter will make it flaky).
- **Pool saturation.** Assert `elapsed < sum(per_file_times)` to prove concurrency is actually happening. More robust than absolute time.
- **Progress bar consistency.** At end of run, `display.progress.tasks[i].completed == display.progress.tasks[i].total` for each task. Catches `on_size_known` / `on_bytes_downloaded` wiring bugs.

## Other behaviors worth covering

- **Idempotency.** Run the same download twice into the same outdir; assert the second run is a no-op (or matches whatever the documented re-fetch policy turns out to be). Bug-prone area.
- **SIGINT mid-run.** Send Ctrl-C while futures are pending; assert both pools shut down within ~5s, no orphan `.part` / `.tmp` files, partial files are either cleaned up or atomically renamed only on success.
- **Disk-full.** Run against a small tmpfs; assert clean `DownloadFailure` reporting, not a stack trace.
- **Outdir layout.** Confirm end-to-end that files land at `outdir/<collection.slug>/<dataset.slug>/<file>`. (A unit test already covers this against mocks.)
- **HTTP-as-directory failure.** Some upstream DB rows store HTTP URLs as directories (`https://.../10242/`). The CLI's HTTP path only handles single-file URLs, so these must surface as a `DownloadFailure` with a sensible reason — not a `ValueError` traceback. Not currently exercised in the standard fixtures; add a dedicated `http-dir-failure` fixture if you want explicit regression coverage.


## Fixture-to-tier mapping

| Fixture | Tier 1 | Tier 2 | Tier 3 | Notes |
|---|---|---|---|---|
| `medium-mixed-paths-collection` | yes | yes | yes | primary integration target — S3 dir, S3 single-object, HTTP single-object, HTTP+S3 in one dataset |
