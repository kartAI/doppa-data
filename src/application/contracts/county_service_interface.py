from abc import ABC, abstractmethod

import geojson

from src.domain.enums import EPSGCode


class ICountyService(ABC):
    @abstractmethod
    def get_county_ids(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_county_polygon_by_id(self, county_id: str, epsg_code: EPSGCode) -> geojson.Feature:
        raise NotImplementedError
