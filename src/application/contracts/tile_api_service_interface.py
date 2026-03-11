from abc import abstractmethod, ABC

from pmtiles.reader import Reader


class ITileApiService(ABC):
    @abstractmethod
    def fetch_vmt_tile(self, z: int, x: int, y: int) -> bytes | None:
        """
        Fetches a tile from the VMT server based on the provided z, x, and y coordinates.
        :param z: Zoom level of the tile
        :param x: X coordinate of the tile
        :param y: Y coordinate of the tile
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_pmtiles_tile(self, reader: Reader, z: int, x: int, y: int) -> bytes | None:
        raise NotImplementedError

    @abstractmethod
    def create_pmtiles_reader(self, pmtiles_url: str) -> Reader:
        raise NotImplementedError
