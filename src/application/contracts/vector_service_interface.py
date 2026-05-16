from abc import ABC, abstractmethod
import geopandas as gpd

from src.domain.enums import EPSGCode


class IVectorService(ABC):
    @abstractmethod
    def compute_partition_key(self, dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Compute the `partition_key` column for each row in the given GeoDataFrame using a precision-3
        geohash over the LAEA Europe centroid of the geometry. Returns a copy of the input frame with
        the `partition_key` column added (or overwritten).
        :param dataframe: GeoDataFrame with a valid geometry column in WGS84.
        :return: Copy of the input frame with a `partition_key` column populated.
        :rtype: gpd.GeoDataFrame
        """
        raise NotImplementedError

    @abstractmethod
    def partition_dataframe(self, dataframe: gpd.GeoDataFrame) -> list[gpd.GeoDataFrame]:
        """
        Splits the given GeoDataFrame into partitions grouped by the computed `partition_key`. The
        partition key is computed via `compute_partition_key` before grouping.
        :param dataframe: GeoDataFrame with a valid geometry column in WGS84.
        :return: List of GeoDataFrame partitions, one per distinct `partition_key` value.
        :rtype: list[gpd.GeoDataFrame]
        """
        raise NotImplementedError

    @abstractmethod
    def clip_dataframes_to_wkb(
            self,
            dataframes: list[gpd.GeoDataFrame],
            wkb: bytes,
            epsg_code: EPSGCode
    ) -> gpd.GeoDataFrame:
        """
        Clips each input GeoDataFrame to the geometry defined by the given WKB and concatenates the
        results into a single GeoDataFrame. The clip is performed via DuckDB `ST_Intersects`. Empty
        input frames are skipped, and the optional columns `feature_update_time` and
        `feature_capture_time` are added as NULL when missing so all partitions share the same schema.
        :param dataframes: List of GeoDataFrames to clip. Must include `external_id`, `geometry`, and
            `building_id` columns.
        :param wkb: WKB representation of the clip geometry.
        :param epsg_code: EPSG code for the coordinate reference system of the returned GeoDataFrame.
        :return: Concatenated GeoDataFrame containing only the rows whose geometry intersects the clip
            geometry, with geometries decoded from WKB.
        :rtype: gpd.GeoDataFrame
        """
        raise NotImplementedError
