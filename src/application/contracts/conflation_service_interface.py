from abc import ABC, abstractmethod

import pandas as pd
import geopandas as gpd

from src.domain.enums import Theme


class IConflationService(ABC):
    @abstractmethod
    def get_fkb_osm_id_relations(self, release: str, theme: Theme, region: str) -> pd.DataFrame:
        """
        Creates a DataFrame relating FKB and OSM building IDs for the given release, theme, and region.
        Pairs are produced by spatially joining FKB and OSM buildings on a centroid grid: rows with a
        NULL `osm_id` are FKB-only buildings, rows with a NULL `fkb_id` are OSM-only buildings, and
        rows with both IDs are overlapping buildings whose IoU exceeds 0.70.
        :param release: Release identifier on the format 'yyyy-mm-dd.x'.
        :param theme: Theme enum.
        :param region: Region ID, e.g. '03' for Oslo.
        :return: DataFrame with `fkb_id` and `osm_id` columns relating the two datasets.
        :rtype: pd.DataFrame
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
        Selects geometries based on the FKB/OSM ID relations and merges them into a single dataset.
        FKB geometries are preferred when both sources match (rows with both IDs), OSM-only buildings
        are taken from the OSM source, and FKB-only buildings are taken from the FKB source. The merged
        dataset is partitioned by `partition_key`.
        :param release: Release identifier on the format 'yyyy-mm-dd.x'.
        :param region: Region ID, e.g. '03' for Oslo.
        :param theme: Theme enum.
        :param ids: ID DataFrame relating FKB and OSM IDs, as produced by `get_fkb_osm_id_relations`.
        :return: A list of GeoDataFrames. Each element is a partition keyed by `partition_key`.
        :rtype: list[gpd.GeoDataFrame]
        """
        raise NotImplementedError
