from typing import Any

from pystac import HREF

from src.application.contracts import IStacIOService, IBlobStorageService
from src.domain.enums import StorageContainer


class StacIOService(IStacIOService):
    __blob_storage_service: IBlobStorageService

    def __init__(self, blob_storage_service: IBlobStorageService):
        super().__init__()
        self.__blob_storage_service = blob_storage_service

    def write_text(self, dest: HREF, txt: str, *args: Any, **kwargs: Any) -> None:
        data = txt.encode(encoding="utf-8")
        path = self.strip_path_stem(dest)
        self.__blob_storage_service.upload_file(container_name=StorageContainer.STAC, blob_name=path, data=data)

    def read_text(self, source: HREF, *args: Any, **kwargs: Any) -> str:
        path = self.strip_path_stem(source)
        path = path.lstrip(".")
        data = self.__blob_storage_service.download_file(container_name=StorageContainer.STAC, blob_name=path)

        if data is None:
            raise FileNotFoundError(f"File not found in blob storage: {path}")

        return data.decode(encoding="utf-8")
        # return super().read_text(source=source, *args, **kwargs)

    def strip_path_stem(self, path: str) -> str:
        return path.split("stac/", 1)[1]
