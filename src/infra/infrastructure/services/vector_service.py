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
        dataframe = dataframe.copy()
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

        OPTIONAL_COLS = ["feature_update_time", "feature_capture_time"]

        for i, gdf in enumerate(dataframes):
            if gdf.empty:
                continue

            gdf = gdf.copy()
            gdf["geometry"] = gdf["geometry"].to_wkb()
            view_name = f"gdf_{i}"
            self.__db_context.register(view_name, gdf)

            table_info = self.__db_context.execute(
                f"PRAGMA table_info('{view_name}')"
            ).fetchdf()

            cols = set(table_info["name"].tolist())

            select_parts = [
                "external_id",
                "geometry",
                "building_id",
                "* EXCLUDE(external_id, geometry, building_id)"
            ]

            for col in OPTIONAL_COLS:
                if col in cols:
                    select_parts.append(f"{col} AS {col}")
                else:
                    select_parts.append(f"NULL AS {col}")

            select_clause = ",\n".join(select_parts)

            sql = f"""
                SELECT
                    {select_clause}
                FROM {view_name}
                WHERE ST_Intersects(
                    ST_GeomFromWKB(geometry),
                    ST_GeomFromWKB(?)
                )
            """

            clipped_gdf = self.__db_context.execute(sql, [wkb]).fetchdf()

            clipped_dataframes.append(clipped_gdf)
            self.__db_context.unregister(view_name)

        df = pd.concat(clipped_dataframes, ignore_index=True)
        df["geometry"] = df["geometry"].apply(
            lambda x: bytes(x) if isinstance(x, (bytearray, memoryview)) else x
        )
        df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])

        return gpd.GeoDataFrame(df, geometry="geometry", crs=f"EPSG:{epsg_code.value}")
