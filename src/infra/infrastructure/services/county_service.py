import requests
import geojson

from src import Config
from src.application.contracts import ICountyService
from src.domain.enums import EPSGCode


class CountyService(ICountyService):
    def get_county_ids(self) -> list[str]:
        response = requests.get(f"{Config.GEONORGE_BASE_URL}/fylker")
        response.raise_for_status()
        data = response.json()
        return [item["fylkesnummer"] for item in data]

    def get_county_polygon_by_id(self, county_id: str, epsg_code: EPSGCode) -> geojson.Feature:
        response = requests.get(f"{Config.GEONORGE_BASE_URL}/fylker/{county_id}/omrade?utkoordsys={epsg_code}")
        response.raise_for_status()
        return geojson.loads(response.text)
