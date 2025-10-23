from abc import ABC, abstractmethod
import pandas as pd


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
