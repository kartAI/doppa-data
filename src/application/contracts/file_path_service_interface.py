from abc import ABC, abstractmethod
from typing import Literal

from src.domain.enums import Theme, StorageContainer


class IFilePathService(ABC):
    # TODO: Convert this to static method
    @abstractmethod
    def create_dataset_blob_path(
            self,
            release: str,
            theme: Theme,
            region: str,
            file_name: str,
            **kwargs
    ) -> str:
        """
        Creates a storage account file path to a dataset blob. On the format `release/{release}/**kwargs/theme={theme}/region={region}/{file_name}`

        :param release: Release version in the format 'yyyy-mm-dd.x'
        :param theme: Theme enum value
        :param region: A region in Norway is defined as county. For example '03' for Oslo
        :param file_name: File name to store. Must be in the format 'part_xxxxx.parquet'
        :param kwargs: Additional keyword arguments that will be added between release and theme
        :return: Storage path
        :rtype: str
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def get_blob_file_name(file_path: str) -> str:
        """
        Gets the ending of a blob path, starting from the file name.

        :param file_path: File name to store. Must be in the format 'part_xxxxx.parquet'
        :return: Storage path ending
        :rtype: str
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def remove_blob_file_name_from_path(file_path: str) -> str:
        """
        Removes the file name from a blob path, returning the directory path.
        :param file_path: File path including the file name.
        :return: File path without the file name.
        """

    @staticmethod
    @abstractmethod
    def create_blob_path(*args) -> str:
        """
        Creates a storage account file path by joining the provided arguments with '/'.

        :param args: Parts of the path to be joined
        :return: Storage path
        :rtype: str
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def create_virtual_filesystem_path(
            storage_scheme: Literal["az"],
            container: StorageContainer,
            release: str,
            theme: Theme,
            region: str,
            file_name: str,
            **kwargs: str
    ) -> str:
        """
        Creates a virtual filesystem path for accessing files in a storage account.
        :param storage_scheme: Storage scheme, e.g., "az" for Azure Blob Storage
        :param container: Name of storage container
        :param release: Release on the format 'yyyy-mm-dd.x'
        :param theme: Theme enum value
        :param region: Region code, i.e '03' for Oslo
        :param file_name: File name to store. Must be in the format 'part_xxxxx.parquet', or '*.parquet' for all files
        :param kwargs: Additional keyword arguments that will be added between release and theme
        :return: Virtual filesystem path
        """
        raise NotImplementedError
