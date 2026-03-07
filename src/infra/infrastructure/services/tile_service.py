import math

from src.application.contracts import ITileService


class TileService(ITileService):
    def lat_lon_to_tile(
            self,
            lat: float,
            lon: float,
            zoom: int,
            bounding_box: tuple[float, float, float, float]
    ) -> tuple[int, int, int]:
        min_lat, min_lon, max_lat, max_lon = bounding_box

        lat = max(min_lat, min(lat, max_lat))
        lon = max(min_lon, min(lon, max_lon))

        lat = max(min(lat, 85.05112878), -85.05112878)

        n = 2 ** zoom

        x = int((lon + 180.0) / 360.0 * n)
        y = int((1.0 - math.log(math.tan(math.radians(lat)) + (1 / math.cos(math.radians(lat)))) / math.pi) / 2.0 * n)

        x = max(0, min(x, n - 1))
        y = max(0, min(y, n - 1))

        return zoom, x, y

    def build_candidate_tiles(
            self,
            min_lat: float,
            min_lon: float,
            max_lat: float,
            max_lon: float,
            zoom: int
    ) -> list[tuple[int, int, int]]:
        _, top_left_x, top_left_y = self.lat_lon_to_tile(
            lat=max_lat,
            lon=min_lon,
            zoom=zoom,
            bounding_box=(min_lat, min_lon, max_lat, max_lon),
        )
        _, bottom_right_x, bottom_right_y = self.lat_lon_to_tile(
            lat=min_lat,
            lon=max_lon,
            zoom=zoom,
            bounding_box=(min_lat, min_lon, max_lat, max_lon),
        )

        min_x = min(top_left_x, bottom_right_x)
        max_x = max(top_left_x, bottom_right_x)
        min_y = min(top_left_y, bottom_right_y)
        max_y = max(top_left_y, bottom_right_y)

        return [
            (zoom, x, y)
            for x in range(min_x, max_x + 1)
            for y in range(min_y, max_y + 1)
        ]
