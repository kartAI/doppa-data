from abc import ABC, abstractmethod


class IMVTService(ABC):
    @abstractmethod
    def get_mvt_tiles(self, z: int, x: int, y: int) -> bytes | None:
        """
        Fetches MVT tiles for the given zoom level (z) and tile coordinates (x, y).
        :param z:
        :param x:
        :param y:
        :return:
        """
        raise NotImplementedError
