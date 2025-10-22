from abc import ABC, abstractmethod

from src.domain.enums import Theme


class IFilePathService(ABC):
    @abstractmethod
    def create_storage_account_file_path(
            self,
            release: str,
            theme: Theme,
            region: str,
            file_name: str,
            prefix: str = None
    ) -> str:
        """
        Creates a storage account file path based on the provided parameters.

        :param release: Release version in the format 'yyyy-mm-dd.x'
        :param theme: Theme enum value
        :param region: A region in Norway is defined as county. For example '03' for Oslo
        :param file_name: File name to store. Must be in the format 'part_xxxxx.parquet'
        :return: Storage path
        :param prefix: Prefix to add at the start of the path
        :rtype: str
        """
        raise NotImplementedError
