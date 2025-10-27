from io import BytesIO

import geopandas as gpd
import pandas as pd

from src.application.contracts import IBytesService


class BytesService(IBytesService):
    @staticmethod
    def convert_bytes_to_df(data: bytes) -> pd.DataFrame:
        return pd.read_parquet(BytesIO(data))

    @staticmethod
    def convert_df_to_bytes(df: pd.DataFrame | gpd.GeoDataFrame) -> bytes:
        raise NotImplementedError
