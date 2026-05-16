from abc import abstractmethod, ABC

from pmtiles.reader import Reader


class ITileApiService(ABC):
    @abstractmethod
    def fetch_vmt_tile(self, z: int, x: int, y: int) -> bytes | None:
        """
        Fetches a tile from the VMT server based on the provided z, x, and y coordinates. Cache headers
        are set to bypass any intermediate cache.
        :param z: Zoom level of the tile
        :param x: X coordinate of the tile
        :param y: Y coordinate of the tile
        :return: Raw tile bytes, or None when the server returns 404 or an empty response body.
        :rtype: bytes | None
        :raises RuntimeError: If the VMT server cannot be reached or returns a non-404 error status.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_pmtiles_tile(self, reader: Reader, z: int, x: int, y: int) -> bytes | None:
        """
        Fetches a tile from a PMTiles archive via the given reader. The reader resolves the tile's byte
        range and returns its contents.
        :param reader: PMTiles Reader bound to the archive to read from.
        :param z: Zoom level of the tile
        :param x: X coordinate of the tile
        :param y: Y coordinate of the tile
        :return: Raw tile bytes, or None when the tile is not present in the archive.
        :rtype: bytes | None
        """
        raise NotImplementedError

    @abstractmethod
    def create_pmtiles_reader(self, pmtiles_url: str) -> Reader:
        """
        Creates a PMTiles Reader that reads the archive at the given URL using HTTP range requests. The
        underlying byte-source validates that the server returns HTTP 206 Partial Content with the exact
        requested byte range.
        :param pmtiles_url: HTTP(S) URL of the PMTiles archive.
        :return: PMTiles Reader bound to the remote archive.
        :rtype: Reader
        """
        raise NotImplementedError
