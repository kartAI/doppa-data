from geojson import GeoJSON

from src import Config
from src.application.common import BuildingHandler
from src.application.services import OpenStreetMapService


def extract_osm_buildings() -> GeoJSON:
    if not Config.OSM_FILE_PATH.is_file():
        OpenStreetMapService.download_pbf()

    building_handler = BuildingHandler()
    osm_service = OpenStreetMapService(building_handler=building_handler)
    geojson = osm_service.get_geojson()
    return geojson
