from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd
import geopandas as gpd

from src.domain.enums import Theme


class IConflationService(ABC):
    @abstractmethod
    def get_fkb_osm_id_relations(self, release: str, theme: Theme) -> pd.DataFrame:
        """
        Creates a with all IDs from FKB and OSM for a given release and theme, and relates them to each other.
        :param release: Release identifier 'yyyy-mm-dd.x'
        :param theme: Theme enum
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def merge_fkb_osm(self, release: str, theme: Theme, ids: pd.DataFrame) -> Dict[str, list[gpd.GeoDataFrame]]:
        """
        Selects geometries based on priority and merges them.
        :param release: Release identifier 'yyyy-mm-dd.x'
        :param theme: Theme enum
        :param ids: ID dataframe that relates FKB and OSM IDs
        :return: A dictionary with region as key and another dictionary as value. The outer dictionary is keyed by region, and the inner is keyed by the spatial hash
        """
        raise NotImplementedError
