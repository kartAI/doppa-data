import json

from geojson import Feature
from osmium import SimpleHandler
from osmium.geom import GeoJSONFactory
from osmium.osm import Area

from src.application.common import logger


class BuildingHandler(SimpleHandler):
    __geom_factory: GeoJSONFactory
    __buildings: list[Feature]

    def __init__(self):
        super().__init__()
        self.__geom_factory = GeoJSONFactory()
        self.__buildings = []

    @property
    def geom_factory(self) -> GeoJSONFactory:
        return self.__geom_factory

    @property
    def buildings(self) -> list[Feature]:
        return self.__buildings

    def area(self, area: Area) -> None:
        try:
            if "building" in area.tags:
                logger.debug(f"Processing building {area.id}")
                geojson_string = self.geom_factory.create_multipolygon(area)
                geometry = json.loads(geojson_string)

                props: dict[str, str | int | float] = dict(area.tags)
                props["id"] = area.id

                feature = Feature(
                    geometry=geometry,
                    properties=props
                )

                self.buildings.append(feature)
                logger.debug(f"Building {area.id} was successfully processed")
        except Exception as e:
            logger.error(f"Skipping way {area.id} due to geometry error: {e}")
