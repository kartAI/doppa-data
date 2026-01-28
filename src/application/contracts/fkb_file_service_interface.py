from abc import ABC, abstractmethod

import geopandas as gdf


class IFKBFileService(ABC):
    @abstractmethod
    def download_fgb_zip_file(self, path: str) -> bytes:
        """
        Downloads the FlatGeobuf ZIP file from the given path.
        :param path: Path to ZIP file, e.g., URL or local file path.
        :return: ZIP file as bytes.
        """
        raise NotImplementedError

    @abstractmethod
    def open_fgb_file(self, path: str, *layers: str) -> gdf.GeoDataFrame:
        """
        :param path:
        :param layers:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def create_dataframe_from_zip_bytes(self, zip_bytes: bytes) -> gdf.GeoDataFrame:
        """
        Creates a GeoDataFrame from the given ZIP file bytes and specified layers.
        :param zip_bytes: ZIP file as bytes.
        :return: GeoDataFrame containing the extracted data.
        """
        raise NotImplementedError
