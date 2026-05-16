from abc import ABC, abstractmethod


class IMVTService(ABC):
    @abstractmethod
    async def get_mvt_tiles(self, z: int, x: int, y: int) -> bytes | None:
        """
        Fetches an MVT tile for the buildings layer at the given zoom level (z) and tile coordinates
        (x, y). The tile is generated server-side from PostGIS using `ST_AsMVT` and returned as the
        raw protobuf payload.
        :param z: Zoom level of the tile.
        :param x: X coordinate of the tile.
        :param y: Y coordinate of the tile.
        :return: Raw MVT tile bytes, or None when no features intersect the tile envelope.
        :rtype: bytes | None
        """
        raise NotImplementedError
