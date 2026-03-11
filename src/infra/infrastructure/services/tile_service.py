import json
import math

from src import Config
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

    def load_tiles(self, number_of_tiles: int) -> list[tuple[int, int, int]]:
        with Config.MVT_TILES_PATH.open("r", encoding="utf-8") as f:
            raw = f.read()

        if not raw or not raw.strip():
            raise ValueError(f"Tiles JSON at {Config.MVT_TILES_PATH} is empty")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            preview = repr(raw[:200])
            raise ValueError(
                f"Failed to parse tiles JSON at {Config.MVT_TILES_PATH}: {exc}.\n"
                f"File start preview (first 200 chars): {preview}\n"
                "Common causes: file saved with wrong encoding/BOM (we use utf-8-sig),"
                " extra characters before JSON (e.g. stray comma), or invalid JSON syntax."
            ) from exc

        if not isinstance(data, list):
            raise ValueError(f"Tiles JSON must be a list, got {type(data).__name__}")

        tiles: list[tuple[int, int, int]] = []
        for idx, item in enumerate(data):
            if not isinstance(item, (list, tuple)) or len(item) != 3:
                raise ValueError(f"Tile at index {idx} must be a 3-element list/tuple, got: {item}")
            try:
                z, x, y = int(item[0]), int(item[1]), int(item[2])
            except Exception as exc:
                raise ValueError(f"Tile at index {idx} contains non-integer values: {item}") from exc
            tiles.append((z, x, y))

        return (tiles * ((number_of_tiles // len(tiles)) + 1))[:number_of_tiles]
