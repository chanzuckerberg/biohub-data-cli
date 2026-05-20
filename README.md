# data-cli

[![CI](https://github.com/chanzuckerberg/biohub-data-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/chanzuckerberg/biohub-data-cli/actions/workflows/ci.yml)
[![Coverage](https://github.com/chanzuckerberg/biohub-data-cli/raw/badges/coverage.svg)](https://github.com/chanzuckerberg/biohub-data-cli/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/biohub-data-cli.svg)](https://pypi.org/project/biohub-data-cli/)

Command-line tool for downloading datasets published by CZ Biohub. Resolves a collection ID to its constituent datasets and pulls every file in parallel from S3 and HTTP, with progress bars, size estimates, and dry-run accounting.

## Installation

```bash
pip install biohub-data-cli
```

This installs the `ops-data` command. Python 3.10+ is required.

## Quick start

```bash
# See what a collection contains without downloading
ops-data download collection <collection-id> --dry-run

# Download a collection to the current directory
ops-data download collection <collection-id>

# Download multiple collections to a specific directory, skip the prompt
ops-data download collection <id-a> <id-b> -o ./data -y
```

Files land under `<outdir>/<collection-slug>/<dataset-slug>/`.

## Commands

### `ops-data download collection IDS...`

Download one or more collections by ID.

| Option | Description |
|--------|-------------|
| `-o, --outdir PATH` | Output directory. Defaults to `.`. |
| `-y, --yes` | Skip the size-estimate confirmation prompt. |
| `--dry-run` | Print per-dataset size statistics without downloading. Mutually exclusive with `-y`. |

**Dry run** resolves every S3 URI (listing prefixes, heading objects) to report exact byte totals per dataset. HTTP URLs are not sized during dry run and surface as a warning in the summary. A non-zero exit indicates at least one S3 URI could not be resolved.

**Confirmation prompt** shows the aggregate size estimate before any bytes move. Pass `-y` to skip it in scripts.

**Failures** are collected and reported at the end. The process exits non-zero if any download failed, but other downloads continue — one bad URL won't abort the run.

## Development

This project uses [`uv`](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies (including dev extras)
uv sync

# Run tests
uv run pytest

# Run tests with coverage report
uv run pytest --cov=biohub_data_cli --cov-report=term-missing

# Run the CLI from a checkout
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
