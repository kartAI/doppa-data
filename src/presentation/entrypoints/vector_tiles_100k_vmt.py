import json

from dependency_injector.wiring import inject, Provide

from src import Config
from src.application.common import logger
from src.application.common.monitor_network import monitor_network
from src.application.contracts import ITileApiService
from src.infra.infrastructure import Containers

TOTAL_REQUESTS: int = 100_000


def _load_tiles() -> list[tuple[int, int, int]]:
    # Read file using utf-8-sig to handle BOM if present, then parse explicitly
    with Config.MVT_TILES_PATH.open("r", encoding="utf-8") as f:
        raw = f.read()

    if not raw or not raw.strip():
        raise ValueError(f"Tiles JSON at {Config.MVT_TILES_PATH} is empty")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        # Include a short preview of the file to help debugging without dumping everything
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

    return tiles


def vector_tiles_100k_vmt() -> None:
    existing_tiles = _load_tiles()
    logger.info(f"Loaded {len(existing_tiles)} tiles from {Config.MVT_TILES_PATH}")

    if not existing_tiles:
        logger.error("No tiles found in the tiles JSON; aborting benchmark.")
        return

    tiles: list[tuple[int, int, int]] = (
            existing_tiles * ((TOTAL_REQUESTS // len(existing_tiles)) + 1)
    )[:TOTAL_REQUESTS]

    logger.info(f"Benchmark will execute {len(tiles)} requests.")
    _benchmark(tiles=tiles)


@inject
@monitor_network(query_id="vector-tiles-100k-vmt")
def _benchmark(
        tiles: list[tuple[int, int, int]],
        tile_api_service: ITileApiService = Provide[Containers.tile_api_service]
) -> None:
    for tile in tiles:
        z, x, y = tile
        tile_api_service.fetch_vmt_tile(z=z, x=x, y=y)
