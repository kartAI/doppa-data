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
        Converts latitude and longitude to tile coordinates (z, x, y) at a given zoom level. The input
        coordinates are clamped to the given bounding box and to the Web Mercator latitude limits
        (±85.05112878).
        :param lat: Latitude in degrees.
        :param lon: Longitude in degrees.
        :param zoom: Zoom level (0-22).
        :param bounding_box: Bounding box defined as a tuple (min_lat, min_lon, max_lat, max_lon) used
            to clamp the input coordinates.
        :return: Tile coordinates (z, x, y) corresponding to the given latitude and longitude at the
            specified zoom level.
        :rtype: tuple[int, int, int]
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
        Builds a list of candidate tile coordinates (z, x, y) that cover the bounding box defined by
        the given latitude and longitude.
        :param min_lat: Minimum latitude in degrees.
        :param min_lon: Minimum longitude in degrees.
        :param max_lat: Maximum latitude in degrees.
        :param max_lon: Maximum longitude in degrees.
        :param zoom: Zoom level (0-22).
        :return: List of tile coordinates (z, x, y) that cover the bounding box.
        :rtype: list[tuple[int, int, int]]
        """
        raise NotImplementedError

    @abstractmethod
    def load_tiles(self, number_of_tiles: int) -> list[tuple[int, int, int]]:
        """
        Loads valid VMT tile coordinates (z, x, y) from a predefined JSON source on disk. The
        implementation parses and validates the file, then cycles the loaded tiles so the returned
        list has exactly `number_of_tiles` entries.
        :param number_of_tiles: Number of tile coordinates to return. Tiles are repeated cyclically
            when the source contains fewer than `number_of_tiles` entries.
        :return: List of valid tile coordinates (z, x, y) of length `number_of_tiles`.
        :rtype: list[tuple[int, int, int]]
        :raises ValueError: If the source file is empty, not valid JSON, not a list, or contains
            malformed tile entries.
        """
        raise NotImplementedError
