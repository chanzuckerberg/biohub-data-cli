from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class DownloadFailure:
    dataset_name: str
    url: str
    reason: str


class LocationInfo(BaseModel):
    url: str
    size: int | None = None


class DownloadInfo(BaseModel):
    locations: list[LocationInfo]
    total_size: int | None = None
    cli_download: bool
    direct_download: bool


class Dataset(BaseModel):
    id: str
    name: str
    namespace: str
    download_info: DownloadInfo
