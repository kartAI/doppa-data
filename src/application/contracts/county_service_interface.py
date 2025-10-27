from abc import ABC, abstractmethod

from src.domain.enums import EPSGCode


class ICountyService(ABC):
    @abstractmethod
    def get_county_ids(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_county_wkb_by_id(self, county_id: str, epsg_code: EPSGCode) -> bytes:
        raise NotImplementedError
