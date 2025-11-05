from io import BytesIO

import geopandas as gpd
from duckdb import DuckDBPyConnection
from pystac import Catalog, Collection

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IOpenStreetMapService, IOpenStreetMapFileService, IFilePathService, IBlobStorageService, ICountyService,
    IVectorService, IStacService
)
from src.domain.enums import Theme, StorageContainer, EPSGCode, DataSource


class OpenStreetMapService(IOpenStreetMapService):
    __db_context: DuckDBPyConnection
    __osm_file_service: IOpenStreetMapFileService
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService
    __county_service: ICountyService
    __vector_service: IVectorService
    __stac_service: IStacService

    def __init__(
            self,
            db_context: DuckDBPyConnection,
            osm_file_service: IOpenStreetMapFileService,
            file_path_service: IFilePathService,
            blob_storage_service: IBlobStorageService,
            county_service: ICountyService,
            vector_service: IVectorService,
            stac_service: IStacService
    ):
        self.__db_context = db_context
        self.__osm_file_service = osm_file_service
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service
        self.__county_service = county_service
        self.__vector_service = vector_service
        self.__stac_service = stac_service

    def process_osm_dataset(self, catalog: Catalog, release: str) -> None:
        if not Config.OSM_FILE_PATH.is_file():
            raise FileNotFoundError(
                "Failed to find OSM-dataset. Ensure that it has been installed to the correct location"
            )

        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")
        self.__osm_file_service.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        self.__osm_file_service.post_apply_file_cleanup()
        logger.info(f"Batched OSM-dataset into {len(self.__osm_file_service.batches)} batches.")

        county_ids = self.__county_service.get_county_ids()

        building_collection = self.__stac_service.create_collection(
            release=release,
            theme=Theme.BUILDINGS,
            start_time=None,
        )

        catalog.add_child(building_collection)

        for county_id in county_ids:
            logger.info(f"Processing county '{county_id}'")
            self.process_buildings_in_region(
                release_catalog=catalog,
                theme_collection=building_collection,
                region=county_id,
                release=release,
                buildings=self.__osm_file_service.batches
            )

        logger.info(f"Extraction completed")

    def process_buildings_in_region(
            self,
            release_catalog: Catalog,
            theme_collection: Collection,
            region: str,
            release: str,
            buildings: list[gpd.GeoDataFrame]
    ) -> None:
        county_wkb, county_geojson = self.__county_service.get_county_wkb_by_id(
            county_id=region,
            epsg_code=EPSGCode.WGS84
        )

        county_gdf = self.__vector_service.clip_dataframes_to_wkb(
            buildings,
            county_wkb,
            epsg_code=EPSGCode.WGS84
        )

        logger.info(f"County '{region}' has {len(county_gdf)} features after clipping.")
        county_partitions = self.__vector_service.partition_dataframe(
            dataframe=county_gdf,
            batch_size=Config.OSM_FEATURE_BATCH_SIZE
        )

        region_item = self.__stac_service.create_item(
            region=region,
            data_source=DataSource.OSM,
            geometry=county_geojson,
            bbox=None,  # TODO: Calculate bounding boxes for both polygons and multipolygons
            collection=theme_collection,
            properties={
                "crs": "EPSG:4326",
                "data_source": DataSource.OSM.value,
                "feature_count": len(county_gdf),
            },
        )

        # assets_paths = self.upload(release=release, region=region, partitions=county_partitions)
        assets_paths = self.__blob_storage_service.upload_blobs_as_parquet(
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            partitions=county_partitions,
            dataset="osm"
        )

        for asset_path in assets_paths:
            asset = self.__stac_service.create_asset(asset_path=asset_path)
            region_item = self.__stac_service.add_asset_to_item(
                item=region_item,
                key=self.__file_path_service.get_blob_file_name(asset_path),
                asset=asset
            )

        self.__stac_service.add_item_to_collection(theme_collection, region_item)
