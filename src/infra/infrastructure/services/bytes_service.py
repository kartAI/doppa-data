from io import BytesIO

import geopandas as gpd
import pandas as pd

from src.application.contracts import IBytesService
from src.domain.enums import EPSGCode


class BytesService(IBytesService):

    @staticmethod
    def convert_parquet_bytes_to_df(data: bytes) -> pd.DataFrame:
        return pd.read_parquet(BytesIO(data))

    @staticmethod
    def convert_fgb_bytes_to_gdf(
            layers: list[bytes],
            crs_in: EPSGCode = EPSGCode.WGS84,
            crs_out: EPSGCode = EPSGCode.WGS84
    ) -> gpd.GeoDataFrame:
        gdfs: list[gpd.GeoDataFrame] = []
        for layer in layers:
            gdf = gpd.read_file(layer, engine="pyogrio")
            gdfs.append(gdf)

        combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=crs_in.value)
        if crs_in != crs_out:
            combined_gdf = combined_gdf.to_crs(crs_out.value)

        return combined_gdf

    @staticmethod
    def convert_df_to_bytes(df: pd.DataFrame | gpd.GeoDataFrame) -> bytes:
        raise NotImplementedError
