from typing import Any

import requests
import shapely
from duckdb import DuckDBPyConnection
from shapely.geometry import shape

from src import Config
from src.application.contracts import ICountyService
from src.domain.enums import EPSGCode


class CountyService(ICountyService):
    __db_context: DuckDBPyConnection

    def __init__(self, db_context: DuckDBPyConnection):
        self.__db_context = db_context

    def get_county_ids(self) -> list[str]:
        response = requests.get(f"{Config.GEONORGE_BASE_URL}/fylker?sorter=fylkesnummer")
        response.raise_for_status()
        data = response.json()
        return [item["fylkesnummer"] for item in data]

    def get_county_wkb_by_id(self, county_id: str, epsg_code: EPSGCode) -> tuple[bytes, dict[str, Any]]:
        response = requests.get(
            f"{Config.GEONORGE_BASE_URL}/fylker/{county_id}/omrade?utkoordsys={epsg_code.value}"
        )
        response.raise_for_status()

        data = response.json()
        geom_data = data["omrade"]
        geom = shape(geom_data)
        wkb_data = shapely.to_wkb(geom)

        geo_json = {
            "type": geom_data["type"],
            "coordinates": geom_data["coordinates"]
        }

        return wkb_data, geo_json
