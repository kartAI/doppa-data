from src.application.common import BuildingHandler
from src.application.services import OpenStreetMapService
from src.infra.persistence.context import duckdb_context

building_handler = BuildingHandler()
osm_service = OpenStreetMapService(db_context=duckdb_context, building_handler=building_handler)


def extract_osm_buildings() -> None:
    OpenStreetMapService.download_pbf()
    osm_service.create_osm_parquet_file()
