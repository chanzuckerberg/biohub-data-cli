# Integration test strategy

How we verify a real end-to-end download against fixtures in `tests/integration/fixtures/`. Unit tests in `tests/*_test.py` cover wiring and edge cases against mocks; this document covers the live-network/live-S3 layer.

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
- **HTTP-as-directory failure.** The real cryoet DB rows store HTTP URLs as directories (`https://.../10242/`). The CLI's HTTP path only handles single-file URLs, so these must surface as a `DownloadFailure` with a sensible reason — not a `ValueError` traceback. Not currently exercised in the standard fixtures (the `cryoet-*` fixtures are S3-only for clean test runs); add a dedicated `http-dir-failure` fixture if you want explicit regression coverage.


## Fixture-to-tier mapping

| Fixture | Tier 1 | Tier 2 | Tier 3 | Notes |
|---|---|---|---|---|
| `tiny-images-collection` | yes | skip | yes | smoke test, runs in seconds |
| `medium-mixed-paths-collection` | yes | yes | yes | primary integration target |
| `large-mixed-paths-collection` | yes | size-only | yes | weekly / on-demand |
| `mixed-protocol-collection` | yes | yes | — | protocol routing coverage |
| `extra-large-collection` | dry-run only | — | listing time | scheduled CI job |
| `cryoet-small-collection-10042` | yes | — | — | real-DB shape, S3-only, ~10 GB; `slow` marker |
| `cryoet-medium-collection-10055` | yes | — | — | real-DB shape, ~91 GB; `slow` marker |
| `cryoet-large-collection-10031` | yes | — | — | real-DB shape, ~637 GB; `slow` marker, on-demand |

### Note: HTTP intentionally stripped from cryoet fixtures

The real DB rows for cryoet datasets carry both an `s3://` URL and a parallel `https://` URL pointing at the same content. We deliberately drop the HTTP entry in these fixtures because:

1. **Not on the OPS-launch critical path.** HTTP is a lower-priority transport for the initial OPS data launch; S3 is the primary supported path. Spending CI time on it now is premature.
2. **Folder-shaped HTTP URLs aren't supported.** The CLI's HTTP downloader handles single-file URLs only — it has no listing semantics. The DB stores HTTP URLs as directories (`https://.../10242/`), which the CLI cannot expand. Including them would just produce expected failures and obscure real regressions.

If/when HTTP becomes a supported folder transport, restore the HTTPS entries (or add a parallel `cryoet-http-*` fixture set) and update the test parametrize list.
