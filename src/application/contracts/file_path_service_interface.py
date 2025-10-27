from abc import ABC, abstractmethod

from src.domain.enums import Theme


class IFilePathService(ABC):
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
        :return: Storage path
        :param kwargs: Additional keyword arguments that will be added between release and theme
        :rtype: str
        """
        raise NotImplementedError
