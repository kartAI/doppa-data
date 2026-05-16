from abc import ABC, abstractmethod
from typing import Any

from src.domain.enums import EPSGCode


class ICountyService(ABC):
    @abstractmethod
    def get_county_ids(self) -> list[str]:
        """
        Returns the list of Norwegian county IDs sorted by `fylkesnummer`. The IDs are fetched from the
        Geonorge administrative units API.
        :return: List of county IDs as two-digit strings, e.g. ['03', '11', ...].
        :rtype: list[str]
        """
        raise NotImplementedError

    @abstractmethod
    def get_county_polygons_by_id(self, county_id: str, epsg_code: EPSGCode) -> tuple[bytes, dict[str, Any]]:
        """
        Returns the polygon geometry for the given county. The geometry is fetched from the cached metadata
        blob if available, otherwise it is downloaded from the Geonorge API and cached for future calls.
        :param county_id: County ID as a two-digit string, e.g. '03' for Oslo.
        :param epsg_code: EPSG code for the coordinate reference system of the returned geometry.
        :return: Tuple containing the WKB representation of the polygon and its GeoJSON dictionary
            with `type` and `coordinates` keys.
        :rtype: tuple[bytes, dict[str, Any]]
        """
        raise NotImplementedError
