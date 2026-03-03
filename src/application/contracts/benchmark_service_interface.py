from abc import ABC, abstractmethod
from pathlib import Path


class IBenchmarkService(ABC):
    @abstractmethod
    def download_parquet_as_shapefile_locally(self, virtual_file_path: str, save_path: Path) -> None:
        """
        Download the parquet files from blob storage and save them locally as shapefiles. This is intended for use
        BEFORE the benchmarking.
        :param virtual_file_path: The virtual file path in blob storage where the parquet files are stored.
        :param save_path: The local path where the shapefiles will be saved.
        :return: None
        """
        raise NotImplementedError
