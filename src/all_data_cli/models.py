from dataclasses import dataclass

from pydantic import BaseModel


class Dataset(BaseModel):
    """A single downloadable unit.

    Maps to upstream's Asset table (will be renamed Dataset per AIP-181).
    `urls` is a list to insulate callers from upstream field-name changes
    and to allow future expansion (e.g. supplementary files).
    """

    id: str
    slug: str
    title: str
    file_format: str
    file_size_bytes: int | None = None
    urls: list[str]


class Collection(BaseModel):
    """A grouping of datasets.

    Maps to upstream's Dataset table (will be renamed Collection per AIP-181).
    """

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
