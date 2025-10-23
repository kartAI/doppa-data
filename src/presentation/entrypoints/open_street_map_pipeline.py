from dependency_injector.wiring import inject, Provide

from src.application.contracts import IOpenStreetMapService, IOpenStreetMapFileService
from src.infra.infrastructure import Containers


@inject
def extract_osm_buildings(
        osm_service: IOpenStreetMapService = Provide[Containers.open_street_map_service],
        osm_file_service: IOpenStreetMapFileService = Provide[Containers.osm_file_service]
) -> None:
    osm_file_service.download_pbf()
    osm_service.create_osm_parquet_file()
