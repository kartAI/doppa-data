from abc import abstractmethod, ABC

from requests import RequestException, Request


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
