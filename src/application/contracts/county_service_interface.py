from abc import ABC, abstractmethod
from typing import Any

from src.domain.enums import EPSGCode


class ICountyService(ABC):
    @abstractmethod
    def get_county_ids(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_county_polygons_by_id(self, county_id: str, epsg_code: EPSGCode) -> tuple[bytes, dict[str, Any]]:
        raise NotImplementedError
