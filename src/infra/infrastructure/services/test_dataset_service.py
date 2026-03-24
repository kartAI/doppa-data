from typing import Any

import geopandas as gpd
from pystac import Catalog, Collection, Item

from src.application.common import logger
from src.application.contracts import (
    IReleaseService, IStacService, IOpenStreetMapFileService, ICountyService, IOpenStreetMapService, IFKBService,
    IVectorService, IBlobStorageService, IFilePathService, IConflationService
)
from src.application.contracts import ITestDatasetService
from src.domain.enums import EPSGCode, Theme, DataSource, StorageContainer


class TestDatasetService(ITestDatasetService):
    __stac_service: IStacService
    __release_service: IReleaseService
    __vector_service: IVectorService
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService
    __conflation_service: IConflationService
    __county_service: ICountyService
    __fkb_service: IFKBService
    __osm_service: IOpenStreetMapService
    __osm_file_service: IOpenStreetMapFileService

    def __init__(
            self,
            stac_service: IStacService,
            release_service: IReleaseService,
            vector_service: IVectorService,
            file_path_service: IFilePathService,
            blob_storage_service: IBlobStorageService,
            conflation_service: IConflationService,
            county_service: ICountyService,
            fkb_service: IFKBService,
            osm_service: IOpenStreetMapService,
            osm_file_service: IOpenStreetMapFileService
    ):
        self.__stac_service = stac_service
        self.__release_service = release_service
        self.__vector_service = vector_service
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service
        self.__conflation_service = conflation_service
        self.__county_service = county_service
        self.__fkb_service = fkb_service
        self.__osm_service = osm_service
        self.__osm_file_service = osm_file_service

    def run_pipeline(self) -> str:
        latest_release, root_catalog, release_catalog = self.__create_release()
        regions = self.__get_county_ids()

        osm_building_batches = self.__download_and_format_osm_dataset()
        fkb_buildings = self.__download_and_format_fkb_dataset()

        building_collection = self.__create_theme_collection(release_catalog, latest_release, Theme.BUILDINGS)
        region_polygons: dict[str, Any] = {}

        for region in regions:
            osm_partitions, fkb_partitions, region_geojson = self.__clip_and_partition_dataset_to_region(
                osm_building_batches,
                fkb_buildings,
                region
            )

            region_polygons[region] = region_geojson

            osm_region_item = self.__create_region_items(
                theme_collection=building_collection,
                data_source=DataSource.OSM,
                region=region,
                geometry=region_geojson,
                bbox=None,
                epsg_code=EPSGCode.WGS84
            )

            fkb_region_item = self.__create_region_items(
                theme_collection=building_collection,
                data_source=DataSource.FKB,
                region=region,
                geometry=region_geojson,
                bbox=None,
                epsg_code=EPSGCode.WGS84
            )

            osm_blob_paths = self.__upload_assets_to_blob_storage(
                container=StorageContainer.RAW,
                release=latest_release,
                theme=Theme.BUILDINGS,
                region=region,
                partitions=osm_partitions,
                dataset=DataSource.OSM
            )

            fkb_blob_paths = self.__upload_assets_to_blob_storage(
                container=StorageContainer.RAW,
                release=latest_release,
                theme=Theme.BUILDINGS,
                region=region,
                partitions=fkb_partitions,
                dataset=DataSource.FKB
            )

            self.__add_assets_to_item(osm_region_item, osm_blob_paths)
            self.__add_assets_to_item(fkb_region_item, fkb_blob_paths)

            partitions = self.__conflate_fkb_and_osm_dataset(release=latest_release, region=region)

            conflated_blob_paths = self.__upload_assets_to_blob_storage(
                container=StorageContainer.DATA,
                release=latest_release,
                theme=Theme.BUILDINGS,
                region=region,
                partitions=partitions
            )

            conflated_region_item = self.__create_region_items(
                theme_collection=building_collection,
                data_source=DataSource.CONFLATED,
                region=region,
                geometry=region_polygons[region],
                bbox=None,
                epsg_code=EPSGCode.WGS84
            )

            self.__add_assets_to_item(conflated_region_item, conflated_blob_paths)

        self.__save_catalog(catalog=root_catalog, release=latest_release)
        return latest_release

    def __create_release(self) -> tuple[str, Catalog, Catalog]:
        root_catalog = self.__stac_service.get_catalog_root()
        current_release = self.__release_service.create_release()
        release_catalog = self.__stac_service.create_release_catalog(root_catalog=root_catalog, release=current_release)
        return current_release, root_catalog, release_catalog

    def __get_county_ids(self) -> list[str]:
        return self.__county_service.get_county_ids()

    def __download_and_format_osm_dataset(self) -> list[gpd.GeoDataFrame]:
        # osm_file_service.download_pbf()
        return self.__osm_service.create_building_batches()

    def __create_theme_collection(
            self,
            release_catalog: Catalog,
            release: str,
            theme: Theme,
    ) -> Collection:
        theme_collection = self.__stac_service.create_collection(
            release=release,
            theme=theme,
            start_time=None
        )

        release_catalog.add_child(theme_collection)
        return theme_collection

    def __download_and_format_fkb_dataset(self) -> list[gpd.GeoDataFrame]:
        fkb_dataset = self.__fkb_service.extract_fkb_data()
        # building_polygons = fkb_service.create_building_polygons(gdf=fkb_dataset, crs=EPSGCode.WGS84)
        return [fkb_dataset]

    def __clip_and_partition_dataset_to_region(
            self,
            osm_batches: list[gpd.GeoDataFrame],
            fkb_batches: list[gpd.GeoDataFrame],
            region: str,
    ) -> tuple[list[gpd.GeoDataFrame], list[gpd.GeoDataFrame], dict[str, Any]]:
        polygon_wkb, polygon_geojson = self.__county_service.get_county_polygons_by_id(
            county_id=region,
            epsg_code=EPSGCode.WGS84
        )

        osm_county_dataset = self.__vector_service.clip_dataframes_to_wkb(
            osm_batches, polygon_wkb,
            epsg_code=EPSGCode.WGS84
        )

        fkb_county_dataset = self.__vector_service.clip_dataframes_to_wkb(
            fkb_batches, polygon_wkb,
            epsg_code=EPSGCode.WGS84
        )

        logger.info(
            f"Region '{region}' has {len(osm_county_dataset)} and {len(fkb_county_dataset)} features from the OSM- and FKB-datasets"
        )

        osm_partitions = self.__vector_service.partition_dataframe(osm_county_dataset)
        fkb_partitions = self.__vector_service.partition_dataframe(fkb_county_dataset)

        return osm_partitions, fkb_partitions, polygon_geojson

    def __conflate_fkb_and_osm_dataset(
            self,
            release: str,
            region: str,
    ) -> list[gpd.GeoDataFrame]:
        relation_ids = self.__conflation_service.get_fkb_osm_id_relations(
            release=release,
            theme=Theme.BUILDINGS,
            region=region
        )

        conflated_partitions = self.__conflation_service.merge_fkb_osm(
            release=release,
            theme=Theme.BUILDINGS,
            ids=relation_ids,
            region=region
        )
        return conflated_partitions

    def __create_region_items(
            self,
            theme_collection: Collection,
            region: str,
            data_source: DataSource,
            geometry: dict[str, Any],
            bbox: str | None,
            epsg_code: EPSGCode,
    ) -> Item:
        region_item = self.__stac_service.create_item(
            region=region,
            data_source=data_source,
            geometry=geometry,
            bbox=bbox,
            collection=theme_collection,
            properties={
                "crs": f"EPSG:{epsg_code.value}"
            }
        )

        self.__stac_service.add_item_to_collection(theme_collection, region_item)
        return region_item

    def __upload_assets_to_blob_storage(
            self,
            container: StorageContainer,
            release: str,
            theme: Theme,
            region: str,
            partitions: list[gpd.GeoDataFrame],
            dataset: DataSource = None,
    ) -> list[str]:
        assets = self.__blob_storage_service.upload_blobs_as_parquet(
            container=container,
            release=release,
            theme=theme,
            region=region,
            partitions=partitions,
            **({"dataset": dataset.value} if dataset else {})
        )

        return assets

    def __add_assets_to_item(
            self,
            item: Item,
            asset_blob_paths: list[str],
    ) -> None:
        for asset_blob_path in asset_blob_paths:
            asset = self.__stac_service.create_asset(asset_path=asset_blob_path)
            self.__stac_service.add_asset_to_item(
                item=item,
                key=self.__file_path_service.get_blob_file_name(asset_blob_path),
                asset=asset
            )

    def __save_catalog(
            self,
            catalog: Catalog,
            release: str,
    ) -> None:
        self.__stac_service.save_catalog(catalog, release)
