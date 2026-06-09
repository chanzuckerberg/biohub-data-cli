from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, model_validator


class Dataset(BaseModel):
    """A single downloadable unit."""

    id: str
    slug: str
    title: str
    file_size_bytes: int | None = None
    urls: list[str]

    @model_validator(mode="before")
    @classmethod
    def _wrap_s3_uri(cls, data: dict[str, Any]) -> dict[str, Any]:
        # Backend `/v1/cli/collections/{id}` returns each dataset with a single
        # `s3_uri: str`; lift it into our `urls: list[str]` so download code
        # iterates one field.
        if "s3_uri" in data:
            data = {**data, "urls": [data["s3_uri"]]}
        return data


class Collection(BaseModel):
    """A grouping of datasets."""

    id: str
    slug: str
    title: str
    datasets: list[Dataset]


@dataclass
class DownloadFailure:
    collection_slug: str
    dataset_slug: str
    url: str
    reason: str


@dataclass(frozen=True)
class DownloadResult:
    """Outcome of a single file download."""

    size: int | None = None
    failure: DownloadFailure | None = None

    @classmethod
    def succeeded(cls, size: int) -> "DownloadResult":
        return cls(size=size)

    @classmethod
    def failed(cls, failure: DownloadFailure) -> "DownloadResult":
        return cls(failure=failure)

    @property
    def ok(self) -> bool:
        return self.failure is None


@dataclass
class DatasetStats:
    """Per-dataset aggregate of a dry-run S3 resolution.

    `n_failed_uris > 0` means `total_bytes` is partial — at least one of the
    dataset's S3 URIs couldn't be listed/headed, so its files aren't counted
    in the sum. `n_http_urls_skipped > 0` means the dataset also has HTTP
    URLs that dry-run sizing does not size today; they're reported as a
    warning in the summary rather than included in `total_bytes`.
    """

    collection_slug: str
    dataset_slug: str
    total_bytes: int
    n_failed_uris: int
    n_http_urls_skipped: int


@dataclass
class DryRunAggregate:
    """Grand totals across a dry run's per-dataset stats."""

    n_collections: int
    n_datasets: int
    total_bytes: int
    n_failed_uris: int
    n_http_urls_skipped: int
