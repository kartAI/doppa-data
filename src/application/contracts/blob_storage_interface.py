from abc import ABC, abstractmethod

import geopandas as gpd

from src.domain.enums import StorageContainer


class IBlobStorageService(ABC):
    @abstractmethod
    def get_container(self, container_name: StorageContainer) -> any:
        """
        Return an Azure Blob ContainerClient for the given storage container enum.

        :param container_name: Enum identifying the target container.
        :type container_name: StorageContainer
        :return: ContainerClient for the specified container.
        :rtype: ContainerClient
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str:
        """
        Upload binary data to a blob and return its URL.

        :param container_name: Enum identifying the target container.
        :type container_name: StorageContainer
        :param blob_name: Name/path for the uploaded blob.
        :type blob_name: str
        :param data: Binary content to upload.
        :type data: bytes
        :return: URL of the uploaded blob.
        :rtype: str
        """
        raise NotImplementedError

    @abstractmethod
    def delete_file(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def download_file(self) -> gpd.GeoDataFrame:
        raise NotImplementedError
