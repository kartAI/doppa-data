from abc import ABC, abstractmethod


class ITileService(ABC):
    @abstractmethod
    def lat_lon_to_tile(
            self,
            lat: float,
            lon: float,
            zoom: int,
            bounding_box: tuple[float, float, float, float]
    ) -> tuple[int, int, int]:
        """
        Converts latitude and longitude to tile coordinates (x, y) at a given zoom level.
        :param lat: Latitude in degrees
        :param lon: Longitude in degrees
        :param zoom: Zoom level (0-22)
        :param bounding_box: Bounding box defined as a tuple (min_lat, min_lon, max_lat, max_lon) to constrain the input coordinates
        :return: Tile coordinates (x, y) corresponding to the given latitude and longitude at the specified zoom level
        """
        raise NotImplementedError

    @abstractmethod
    def build_candidate_tiles(
            self,
            min_lat: float,
            min_lon: float,
            max_lat: float,
            max_lon: float,
            zoom: int
    ) -> list[tuple[int, int, int]]:
        """
            Builds a list of candidate tile coordinates (x, y) that cover the bounding box defined by the given latitude and longitude.
        :param min_lat: Minimum latitude in degrees
        :param min_lon: Minimum longitude in degrees
        :param max_lat: Maximum latitude in degrees
        :param max_lon: Maximum longitude in degrees
        :param zoom: Zoom level (0-22)
        :return: List of tile coordinates (x, y) that cover the bounding box
        """
        raise NotImplementedError

    @abstractmethod
    def load_tiles(self, number_of_tiles: int) -> list[tuple[int, int, int]]:
        """
        Loads valid VMT tile coordinates (z, x, y) from a predefined source, such as a file or database. The implementation should ensure that only valid tile coordinates are returned, filtering out any invalid entries.
        :return: List of valid tile coordinates (z, x, y) that can be used for benchmarking or other purposes
        """
        raise NotImplementedError
