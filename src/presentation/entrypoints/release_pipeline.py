from typing import Any, Dict

import geopandas as gpd
from dependency_injector.wiring import inject, Provide
from pystac import Catalog, Collection, Item

from src.application.common import logger
from src.application.contracts import (
    IReleaseService, IStacService, IOpenStreetMapFileService, ICountyService, IOpenStreetMapService, IFKBService,
    IVectorService, IBlobStorageService, IFilePathService, IConflationService
)
from src.domain.enums import EPSGCode, Theme, DataSource, StorageContainer
from src.infra.infrastructure import Containers


def run_pipeline() -> None:
    latest_release, root_catalog, release_catalog = create_release()
    regions = get_county_ids()

    osm_building_batches = download_and_format_osm_dataset()
    fkb_buildings = download_and_format_fkb_dataset()

    building_collection = create_theme_collection(release_catalog, latest_release, Theme.BUILDINGS)
    region_polygons: dict[str, Any] = {}

    for region in regions:
        osm_partitions, fkb_partitions, region_geojson = clip_and_partition_dataset_to_region(
            osm_building_batches,
            fkb_buildings,
            region
        )

        region_polygons[region] = region_geojson

        osm_region_item = create_region_items(
            theme_collection=building_collection,
            data_source=DataSource.OSM,
            region=region,
            geometry=region_geojson,
            bbox=None,
            epsg_code=EPSGCode.WGS84
        )

        fkb_region_item = create_region_items(
            theme_collection=building_collection,
            data_source=DataSource.FKB,
            region=region,
            geometry=region_geojson,
            bbox=None,
            epsg_code=EPSGCode.WGS84
        )

        osm_blob_paths = upload_assets_to_blob_storage(
            container=StorageContainer.RAW,
            release=latest_release,
            theme=Theme.BUILDINGS,
            region=region,
            partitions=osm_partitions,
            dataset=DataSource.OSM
        )

        fkb_blob_paths = upload_assets_to_blob_storage(
            container=StorageContainer.RAW,
            release=latest_release,
            theme=Theme.BUILDINGS,
            region=region,
            partitions=fkb_partitions,
            dataset=DataSource.FKB
        )

        add_assets_to_item(osm_region_item, osm_blob_paths)
        add_assets_to_item(fkb_region_item, fkb_blob_paths)

    conflated_partitions = conflate_fkb_and_osm_dataset(release=latest_release)

    for region, partitions in conflated_partitions.items():
        conflated_blob_paths = upload_assets_to_blob_storage(
            container=StorageContainer.DATA,
            release=latest_release,
            theme=Theme.BUILDINGS,
            region=region,
            partitions=partitions
        )

        conflated_region_item = create_region_items(
            theme_collection=building_collection,
            data_source=DataSource.CONFLATED,
            region=region,
            geometry=region_polygons[region],
            bbox=None,
            epsg_code=EPSGCode.WGS84
        )

        add_assets_to_item(conflated_region_item, conflated_blob_paths)

    save_catalog(catalog=root_catalog)


@inject
def create_release(
        release_service: IReleaseService = Provide[Containers.release_service],
        stac_service: IStacService = Provide[Containers.stac_service],
) -> tuple[str, Catalog, Catalog]:
    root_catalog = stac_service.get_catalog_root()
    current_release = release_service.create_release()
    release_catalog = stac_service.create_release_catalog(root_catalog=root_catalog, release=current_release)
    return current_release, root_catalog, release_catalog


@inject
def get_county_ids(
        county_service: ICountyService = Provide[Containers.county_service]
) -> list[str]:
    return county_service.get_county_ids()


@inject
def download_and_format_osm_dataset(
        osm_file_service: IOpenStreetMapFileService = Provide[Containers.osm_file_service],
        osm_service: IOpenStreetMapService = Provide[Containers.open_street_map_service]
) -> list[gpd.GeoDataFrame]:
    osm_file_service.download_pbf()
    return osm_service.create_building_batches()


@inject
def create_theme_collection(
        release_catalog: Catalog,
        release: str,
        theme: Theme,
        stac_service: IStacService = Provide[Containers.stac_service]
) -> Collection:
    theme_collection = stac_service.create_collection(
        release=release,
        theme=theme,
        start_time=None
    )

    release_catalog.add_child(theme_collection)
    return theme_collection


@inject
def download_and_format_fkb_dataset(
        fkb_service: IFKBService = Provide[Containers.fkb_service]
) -> list[gpd.GeoDataFrame]:
    fkb_dataset = fkb_service.extract_fkb_data()
    building_polygons = fkb_service.create_building_polygons(gdf=fkb_dataset, crs=EPSGCode.WGS84)
    return [building_polygons]


@inject
def clip_and_partition_dataset_to_region(
        osm_batches: list[gpd.GeoDataFrame],
        fkb_batches: list[gpd.GeoDataFrame],
        region: str,
        county_service: ICountyService = Provide[Containers.county_service],
        vector_service: IVectorService = Provide[Containers.vector_service],
) -> tuple[list[gpd.GeoDataFrame], list[gpd.GeoDataFrame], dict[str, Any]]:
    polygon_wkb, polygon_geojson = county_service.get_county_polygons_by_id(county_id=region, epsg_code=EPSGCode.WGS84)

    osm_county_dataset = vector_service.clip_dataframes_to_wkb(osm_batches, polygon_wkb, epsg_code=EPSGCode.WGS84)
    fkb_county_dataset = vector_service.clip_dataframes_to_wkb(fkb_batches, polygon_wkb, epsg_code=EPSGCode.WGS84)

    logger.info(
        f"Region '{region}' has {len(osm_county_dataset)} and {len(fkb_county_dataset)} features from the OSM- and FKB-datasets"
    )

    osm_partitions = vector_service.partition_dataframe(osm_county_dataset)
    fkb_partitions = vector_service.partition_dataframe(fkb_county_dataset)

    return osm_partitions, fkb_partitions, polygon_geojson


@inject
def conflate_fkb_and_osm_dataset(
        release: str,
        conflation_service: IConflationService = Provide[Containers.conflation_service]
) -> Dict[str, list[gpd.GeoDataFrame]]:
    relation_ids = conflation_service.get_fkb_osm_id_relations(release=release, theme=Theme.BUILDINGS)
    conflated_partitions = conflation_service.merge_fkb_osm(release=release, theme=Theme.BUILDINGS, ids=relation_ids)
    return conflated_partitions


@inject
def create_region_items(
        theme_collection: Collection,
        region: str,
        data_source: DataSource,
        geometry: dict[str, Any],
        bbox: str | None,
        epsg_code: EPSGCode,
        stac_service: IStacService = Provide[Containers.stac_service]
) -> Item:
    region_item = stac_service.create_item(
        region=region,
        data_source=data_source,
        geometry=geometry,
        bbox=bbox,
        collection=theme_collection,
        properties={
            "crs": f"EPSG:{epsg_code.value}"
        }
    )

    stac_service.add_item_to_collection(theme_collection, region_item)
    return region_item


@inject
def upload_assets_to_blob_storage(
        container: StorageContainer,
        release: str,
        theme: Theme,
        region: str,
        partitions: list[gpd.GeoDataFrame],
        dataset: DataSource = None,
        blob_storage_service: IBlobStorageService = Provide[Containers.blob_storage_service]
) -> list[str]:
    assets = blob_storage_service.upload_blobs_as_parquet(
        container=container,
        release=release,
        theme=theme,
        region=region,
        partitions=partitions,
        **({"dataset": dataset.value} if dataset else {})
    )

    return assets


@inject
def add_assets_to_item(
        item: Item,
        asset_blob_paths: list[str],
        stac_service: IStacService = Provide[Containers.stac_service],
        file_path_service: IFilePathService = Provide[Containers.file_path_service]
) -> None:
    for asset_blob_path in asset_blob_paths:
        asset = stac_service.create_asset(asset_path=asset_blob_path)
        stac_service.add_asset_to_item(
            item=item,
            key=file_path_service.get_blob_file_name(asset_blob_path),
            asset=asset
        )


@inject
def save_catalog(catalog: Catalog, stac_service: IStacService = Provide[Containers.stac_service]) -> None:
    stac_service.save_catalog(catalog)
