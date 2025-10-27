from abc import ABC, abstractmethod
import pandas as pd
import geopandas as gpd


class IBytesService(ABC):
    @staticmethod
    @abstractmethod
    def convert_bytes_to_df(data: bytes) -> pd.DataFrame:
        """
        Converts a byte array to a pandas DataFrame. This assumes that the files a parquet file
        :param data: Byte array of the parquet file. Often downloaded from blob storage.
        :return: Dataframe representation of the byte array.
        :rtype: pd.DataFrame
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def convert_df_to_bytes(df: pd.DataFrame | gpd.GeoDataFrame) -> bytes:
        """
        Converts a Pandas DataFrame or GeoPandas GeoDataFrame to a byte array in parquet format.
        :param df: Pandas DataFrame or GeoPandas GeoDataFrame to convert.
        :return: Byte array representation of the DataFrame in parquet format.
        :rtype: bytes
        """
        raise NotImplementedError
