from osmium import SimpleHandler
from osmium.geom import WKBFactory
from osmium.osm import Area

from src import Config
from src.application.common import logger


class BuildingHandler(SimpleHandler):
    __geom_factory: WKBFactory
    __buildings: list[dict]
    __batches: list[list[dict]]

    def __init__(self):
        super().__init__()
        self.__geom_factory = WKBFactory()
        self.__buildings = []
        self.__batches = []

    @property
    def geom_factory(self) -> WKBFactory:
        return self.__geom_factory

    @property
    def buildings(self) -> list[dict]:
        return self.__buildings

    @buildings.setter
    def buildings(self, buildings: list[dict]) -> None:
        self.__buildings = buildings

    @property
    def batches(self) -> list[list[dict]]:
        return self.__batches

    @batches.setter
    def batches(self, batches: list[list[dict]]) -> None:
        self.__batches = batches

    def area(self, area: Area) -> None:
        try:
            if "building" in area.tags:
                logger.debug(f"Processing building {area.id}")
                feature = self.__create_feature(area)
                self.buildings.append(feature)

                if len(self.buildings) >= Config.OSM_FEATURE_BATCH_SIZE:
                    self.batches.append(self.buildings)
                    self.buildings = []
                    logger.info(f"Created batch #{len(self.batches)}")

                logger.debug(f"Building {area.id} was successfully processed")
        except Exception as e:
            logger.warning(f"Skipping area {area.id} due to geometry error: {e}")

    def __create_feature(self, area: Area) -> dict:
        wkb_bytes = self.geom_factory.create_multipolygon(area)

        props: dict[str, str | int | float] = dict(area.tags)
        props["id"] = area.id

        return {
            "geometry": wkb_bytes,
            **props
        }

    def post_apply_file_cleanup(self):
        if self.buildings:
            self.batches.append(self.buildings)
            self.buildings = []
            logger.info(f"Created batch #{len(self.batches)} in cleanup step")

    def pop_batch_by_index(self, index: int) -> None:
        self.batches.pop(index)
