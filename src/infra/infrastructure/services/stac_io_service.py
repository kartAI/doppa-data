from typing import Any

from pystac import HREF

from src.application.contracts import IStacIOService, IBlobStorageService
from src.domain.enums import StorageContainer


class StacIOService(IStacIOService):
    __blob_storage_service: IBlobStorageService
    __skip_file_download: bool = False

    def __init__(self, blob_storage_service: IBlobStorageService):
        super().__init__()
        self.__blob_storage_service = blob_storage_service

    @property
    def skip_file_download(self) -> bool:
        return self.__skip_file_download

    @skip_file_download.setter
    def skip_file_download(self, value: bool) -> None:
        self.__skip_file_download = value

    def write_text(self, dest: HREF, txt: str, *args: Any, **kwargs: Any) -> None:
        data = txt.encode(encoding="utf-8")
        path = self.strip_path_stem(dest)

        is_file_uploaded = (
                path != "catalog.json" and
                self.__blob_storage_service.is_blob_in_storage_container(
                    container_name=StorageContainer.STAC,
                    blob_name=path
                )
        )

        if is_file_uploaded:
            return

        self.__blob_storage_service.upload_file(container_name=StorageContainer.STAC, blob_name=path, data=data)

    def read_text(self, source: HREF, *args: Any, **kwargs: Any) -> str:
        path = self.strip_path_stem(source)
        path = path.lstrip(".")

        data = self.__blob_storage_service.download_file(container_name=StorageContainer.STAC, blob_name=path)

        if data is None:
            raise FileNotFoundError(f"File not found in blob storage: {path}")

        self.skip_file_download = False
        return data.decode(encoding="utf-8")

    def strip_path_stem(self, path: str) -> str:
        return path.split("stac/", 1)[1]
