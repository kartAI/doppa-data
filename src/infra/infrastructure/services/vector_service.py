import geopandas as gpd
import pandas as pd
from duckdb import DuckDBPyConnection

from src.application.contracts import IVectorService
from src.domain.enums import EPSGCode


class VectorService(IVectorService):
    __db_context: DuckDBPyConnection

    def __init__(self, db_context: DuckDBPyConnection):
        self.__db_context = db_context

    def partition_dataframe(self, dataframe: gpd.GeoDataFrame, batch_size: int) -> list[gpd.GeoDataFrame]:
        if len(dataframe) <= batch_size:
            return [dataframe]
        view_slices = range(0, len(dataframe), batch_size)
        return [dataframe.iloc[i:i + batch_size] for i in view_slices]

    def clip_dataframes_to_wkb(
            self,
            dataframes: list[gpd.GeoDataFrame],
            wkb: bytes,
            epsg_code: EPSGCode
    ) -> gpd.GeoDataFrame:
        clipped_dataframes: list[pd.DataFrame] = []
        for i, gdf in enumerate(dataframes):
            if gdf.empty:
                continue

            gdf = gdf.copy()
            gdf["geometry"] = gdf["geometry"].to_wkb()
            view_name = f"gdf_{i}"
            self.__db_context.register(view_name, gdf)

            clipped_gdf = self.__db_context.execute(f"""
            SELECT * FROM {view_name} 
            WHERE ST_Intersects(
                ST_GeomFromWKB(geometry), 
                ST_GeomFromWKB(?)
            )
            """, [wkb]).fetchdf()

            clipped_dataframes.append(clipped_gdf)
            self.__db_context.unregister(view_name)

        df = pd.concat(clipped_dataframes, ignore_index=True)
        df["geometry"] = df["geometry"].apply(
            lambda x: bytes(x) if isinstance(x, (bytearray, memoryview)) else x
        )

        df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])
        return gpd.GeoDataFrame(df, geometry="geometry", crs=f"EPSG:{epsg_code.value}")
