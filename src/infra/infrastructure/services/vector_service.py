import geopandas as gpd
import pandas as pd
import pygeohash as phg
from duckdb import DuckDBPyConnection

from src import Config
from src.application.contracts import IVectorService
from src.domain.enums import EPSGCode


class VectorService(IVectorService):
    __db_context: DuckDBPyConnection

    def __init__(self, db_context: DuckDBPyConnection):
        self.__db_context = db_context

    def partition_dataframe(self, dataframe: gpd.GeoDataFrame) -> list[gpd.GeoDataFrame]:
        centroids = dataframe.geometry.centroid
        dataframe["partition_key"] = [
            phg.encode(lat, lon, precision=Config.PARTITION_RESOLUTION)
            for lat, lon in zip(centroids.y.values, centroids.x.values)
        ]

        partitions = [
            gpd.GeoDataFrame(partition) for _, partition in
            dataframe.groupby(by="partition_key", sort=False)
        ]

        return partitions

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
