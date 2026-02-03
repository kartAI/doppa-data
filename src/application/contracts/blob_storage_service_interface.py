from abc import ABC, abstractmethod

import geopandas as gpd
from azure.storage.blob import ContainerClient

from src.domain.enums import StorageContainer, Theme


class IBlobStorageService(ABC):
    @abstractmethod
    def ensure_container(self, container_name: StorageContainer) -> None:
        """
        Ensure that the specified storage container exists; create it if it does not.
        :param container_name: Container enum to ensure existence for.
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def get_container(self, container_name: StorageContainer) -> ContainerClient:
        """
        Return an Azure Blob ContainerClient for the given storage container enum.

        :param container_name: Enum identifying the target container.
        :type container_name: StorageContainer
        :return: ContainerClient for the specified container.
        :rtype: ContainerClient
        """
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str | None:
        """
        Upload binary data to a blob and return its URL. Does not upload empty file

        :param container_name: Enum identifying the target container.
        :type container_name: StorageContainer
        :param blob_name: Name/path for the uploaded blob.
        :type blob_name: str
        :param data: Binary content to upload.
        :type data: bytes
        :return: URL of the uploaded blob or None if the file was empty
        :rtype: str
        """
        raise NotImplementedError

    @abstractmethod
    def delete_file(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def download_file(self, container_name: StorageContainer, blob_name: str) -> bytes | None:
        """
        Download bytes of file from Azure Blob Storage
        :param container_name: Container enum to download from.
        :param blob_name: Blob name to download.
        :return: Bytes of the downloaded file.
        :rtype: bytes
        """
        raise NotImplementedError

    @abstractmethod
    def is_blob_in_storage_container(self, container_name: StorageContainer, blob_name: str) -> bool:
        """
        Check if a file exists in the specified container.
        :param container_name: Container enum to check in.
        :param blob_name: Blob name to check for.
        :return: True if the file exists, False otherwise.
        :rtype: bool
        """
        raise NotImplementedError

    @abstractmethod
    def upload_blobs_as_parquet(
            self,
            container: StorageContainer,
            release: str,
            theme: Theme,
            region: str,
            partitions: list[gpd.GeoDataFrame],
            **kwargs: str
    ) -> list[str]:
        """
        Upload multiple GeoDataFrame partitions as blobs to storage as Parquet files
        :param container: Storage container enum to upload to.
        :param release: Release version on the format 'yyyy-mm-dd.x'
        :param theme: Theme enum representing the data theme.
        :param region: County ID, e.g. '03' for Oslo.
        :param partitions: List of GeoDataFrame partitions to upload.
        :return: List of URLs of the uploaded blobs.
        """
        raise NotImplementedError

    @abstractmethod
    def has_files_under_blob_path_base(self, container: StorageContainer, path: str) -> bool:
        """
        Check if there are any blobs under the specified base path in the given container.
        :param container: Storage container enum to list blobs from.
        :param path: Base path to list blobs from.
        :return: True if there are any blobs under the specified base path, False otherwise.
        :rtype: bool
        """
        raise NotImplementedError
