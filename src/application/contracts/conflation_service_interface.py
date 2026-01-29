from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd
import geopandas as gpd

from src.domain.enums import Theme


class IConflationService(ABC):
    @abstractmethod
    def get_fkb_osm_id_relations(self, release: str, theme: Theme, region: str) -> pd.DataFrame:
        """
        Creates a DataFrame with all IDs from FKB and OSM for a given release and theme, and relates them to each other.
        :param release: Release identifier 'yyyy-mm-dd.x'
        :param theme: Theme enum
        :param region: Region ID
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def merge_fkb_osm(
            self,
            release: str,
            region: str,
            theme: Theme,
            ids: pd.DataFrame
    ) -> list[gpd.GeoDataFrame]:
        """
        Selects geometries based on priority and merges them.
        :param release: Release identifier 'yyyy-mm-dd.x'
        :param region: Region ID
        :param theme: Theme enum
        :param ids: ID dataframe that relates FKB and OSM IDs
        :return: A list of GeoDataFrames. Each element is a partition that is partitioned by the geohash
        """
        raise NotImplementedError
