import geopandas as gpd
import pandas as pd
import pyarrow as pa
from duckdb import DuckDBPyConnection
from shapely import from_wkb

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IFKBService, IFKBFileService, IZipService, IBytesService
)
from src.domain.enums import EPSGCode


class FKBService(IFKBService):
    __db_context: DuckDBPyConnection
    __zip_service: IZipService
    __fkb_file_service: IFKBFileService
    __bytes_service: IBytesService

    def __init__(
            self,
            db_context: DuckDBPyConnection,
            zip_service: IZipService,
            fkb_file_service: IFKBFileService,
            bytes_service: IBytesService,
    ) -> None:
        self.__db_context = db_context
        self.__zip_service = zip_service
        self.__fkb_file_service = fkb_file_service
        self.__bytes_service = bytes_service

    def extract_fkb_data(self) -> gpd.GeoDataFrame:
        logger.info(f"Starting extraction of FKB data from Hugging Face.")
        wgs84_gdfs: list[gpd.GeoDataFrame] = []

        for utm32n_path in Config.HUGGING_FACE_UTM32N_PATHS:
            utm32n_zip_file = self.__fkb_file_service.download_fgb_zip_file(utm32n_path)
            utm32n_fgb_layers = self.__zip_service.unzip_flat_geobuf(utm32n_zip_file, *Config.FKB_LAYERS)

            wgs84_gdf = self.__bytes_service.convert_fgb_bytes_to_gdf(
                layers=utm32n_fgb_layers,
                crs_in=EPSGCode.UTM32N,
                crs_out=EPSGCode.WGS84
            )

            wgs84_gdfs.append(wgs84_gdf)

        for utm33n_path in Config.HUGGING_FACE_UTM33N_PATHS:
            utm33n_zip_file = self.__fkb_file_service.download_fgb_zip_file(utm33n_path)
            utm33n_fgb_layers = self.__zip_service.unzip_flat_geobuf(utm33n_zip_file, *Config.FKB_LAYERS)

            wgs84_gdf = self.__bytes_service.convert_fgb_bytes_to_gdf(
                layers=utm33n_fgb_layers,
                crs_in=EPSGCode.UTM33N,
                crs_out=EPSGCode.WGS84
            )

            wgs84_gdfs.append(wgs84_gdf)

        combined_gdf = gpd.GeoDataFrame(pd.concat(wgs84_gdfs, ignore_index=True), crs=EPSGCode.WGS84.value)
        combined_gdf["layer"] = ([gml_id.split(".", 1)[0] for gml_id in combined_gdf["gml_id"].to_numpy()])

        logger.info(f"Downloaded and converted FKB data to WGS84 CRS. Total records: {combined_gdf.shape[0]}")
        return combined_gdf

    def create_building_polygons(self, gdf: gpd.GeoDataFrame, crs: EPSGCode) -> gpd.GeoDataFrame:
        polygons_gdf, points_gdf = self.__create_polygon_and_point_datasets(fkb_dataset=gdf, crs=EPSGCode.WGS84)
        buildings_gdf = self.__find_overlapping_points(polygons=polygons_gdf, points=points_gdf)
        return buildings_gdf

    @staticmethod
    def __find_overlapping_points(polygons: gpd.GeoDataFrame, points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Finds overlapping points within polygons using spatial join.
        :param polygons:
        :param points:
        :return:
        """
        buildings_gdf: gpd.GeoDataFrame = gpd.sjoin(
            polygons,
            points,
            how="inner",
            predicate="contains"
        ).drop(
            columns=["index_right"]
        ).drop_duplicates(
            subset=polygons.columns
        )

        buildings_gdf = FKBService.__cast_to_string(buildings_gdf)
        return buildings_gdf

    def __create_polygon_and_point_datasets(
            self,
            fkb_dataset: gpd.GeoDataFrame,
            crs: EPSGCode
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Creates polygon and point datasets from the FKB dataset.
        :param fkb_dataset: FKB dataset as a GeoDataFrame.
        :return: Tuple containing polygon GeoDataFrame and point GeoDataFrame.
        """
        df = pd.DataFrame(fkb_dataset.copy())
        df["geometry"] = fkb_dataset["geometry"].apply(lambda geom: geom.wkb if geom is not None else None)
        df["geometry"] = df["geometry"].astype("object")
        df = FKBService.__cast_to_string(df)

        table = pa.Table.from_pandas(df=df)
        self.__db_context.register("fkb", table)
        polygonized_df = self.__db_context.execute(
            """
            WITH merged_lines
                     AS (SELECT ST_Union_Agg(ST_Force2D(ST_GeomFromWKB(geometry))) AS geom
                         FROM fkb
                         WHERE layer IN ('Takkant', 'FiktivBygningsavgrensning', 'Bygningsdelelinje')
                           AND geometry IS NOT NULL)
            SELECT ST_AsWKB(ST_Polygonize([geom])) AS geom
            FROM merged_lines;
            """
        ).fetchdf()
        geom = from_wkb(bytes(polygonized_df["geom"].iloc[0]))
        parts = list(geom.geoms)
        polygons = [g for g in parts if g.geom_type in ("Polygon", "MultiPolygon")]

        polygons_gdf = gpd.GeoDataFrame(geometry=polygons, crs=f"EPSG:{crs.value}")
        points_gdf = fkb_dataset[fkb_dataset["layer"].isin(["Bygning", "AnnenBygning"])]

        return polygons_gdf, points_gdf

    @staticmethod
    def __cast_to_string(df: pd.DataFrame | gpd.GeoDataFrame) -> pd.DataFrame | gpd.GeoDataFrame:
        for col in df.columns:
            if df[col].dtype == "object" and col != "geometry":
                df[col] = df[col].astype(str)

        return df
