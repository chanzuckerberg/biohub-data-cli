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
