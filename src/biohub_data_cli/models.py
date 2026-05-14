from dataclasses import dataclass

from pydantic import BaseModel


class Dataset(BaseModel):
    """A single downloadable unit."""

    id: str
    slug: str
    title: str
    file_format: str
    file_size_bytes: int | None = None
    urls: list[str]


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


@dataclass
class DatasetStats:
    """Per-dataset aggregate of a dry-run S3 resolution.

    `n_failed_uris > 0` means `total_bytes` is partial — at least one of the
    dataset's S3 URIs couldn't be listed/headed, so its files aren't counted
    in the sum.
    """

    collection_slug: str
    dataset_slug: str
    total_bytes: int
    n_failed_uris: int
