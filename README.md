# data-cli

[![CI](https://github.com/chanzuckerberg/biohub-data-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/chanzuckerberg/biohub-data-cli/actions/workflows/ci.yml)
[![Coverage](https://github.com/chanzuckerberg/biohub-data-cli/raw/badges/coverage.svg)](https://github.com/chanzuckerberg/biohub-data-cli/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/biohub-data-cli.svg)](https://pypi.org/project/biohub-data-cli/)
[![Python](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fchanzuckerberg%2Fbiohub-data-cli%2Fmain%2Fpyproject.toml)](https://github.com/chanzuckerberg/biohub-data-cli/blob/main/pyproject.toml)

Command-line tool for downloading datasets published by CZ Biohub. Resolves a collection ID to its constituent datasets and downloads files from S3 and HTTP, with progress bars, size estimates, and dry-run accounting.

## Installation

To install the OPS data CLI, run:

```bash
pip install biohub-data-cli
```

## Quick start

See what a collection contains without downloading:

```bash
ops-data download collection <collection-id> --dry-run
```

Download a collection to the current directory:

```bash
ops-data download collection <collection-id>
```

Download multiple collections to a specific directory, skipping the prompt:

```bash
ops-data download collection <id-a> <id-b> -o ./data -y
```

Download only specific datasets from a collection:

```bash
ops-data download collection <collection-id> --dataset dataset-1,dataset-2
```

Files land under `<outdir>/<collection-slug>/<dataset-slug>/`.

## Commands

### `ops-data download collection IDS...`

Download one or more collections by ID.

| Option | Description |
|--------|-------------|
| `-o, --outdir PATH` | Output directory. Defaults to `.`. |
| `-y, --yes` | Skip the size-estimate confirmation prompt. |
| `--dataset SLUGS` | Comma-separated dataset slugs to download a subset of the collection. Only valid with a single collection. |
| `--dry-run` | Print per-dataset size statistics without downloading. Mutually exclusive with `-y`. |
| `--no-resume` | Ignore cached listing state and re-list/re-download from scratch. |

**Dry run** resolves every S3 URI (listing prefixes, heading objects) to report exact byte totals per dataset. HTTP URLs are not sized during dry run and surface as a warning in the summary.

**Filtering datasets** with `--dataset` downloads only the named datasets from a collection instead of all of them, e.g. `--dataset dataset-1,dataset-2`. Slugs are downloaded in the order given, duplicates are ignored, and an unknown slug fails with the list of available slugs. Run `--dry-run` first to see the available slugs. Filtering applies to a single collection, so it can't be combined with multiple IDs.

**Confirmation prompt** shows the aggregate size estimate before any bytes move. Pass `-y` to skip it in scripts.

**Failures** are collected and reported at the end. The process exits non-zero if any download failed, but other downloads continue — one bad URL won't abort the run.

## Development

This project uses [`uv`](https://docs.astral.sh/uv/) for dependency management.

Install dependencies (including dev extras):

```bash
uv sync
```

Run tests:

```bash
uv run pytest
```

Run tests with coverage report:

```bash
uv run pytest --cov=biohub_data_cli --cov-report=term-missing
```

Run the CLI from a checkout:

```bash
uv run ops-data --help
```

### Integration tests

Tests marked `integration` hit real S3 buckets / HTTP servers and are deselected by default. Run them explicitly:

```bash
uv run pytest -m integration
```

## Code of Conduct

This project adheres to the Contributor Covenant [code of conduct](https://github.com/chanzuckerberg/.github/blob/master/CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [opensource@chanzuckerberg.com](mailto:opensource@chanzuckerberg.com).

## Reporting Security Issues

If you believe you have found a security issue, please responsibly disclose by contacting us at [security@chanzuckerberg.com](mailto:security@chanzuckerberg.com).
