import random
from itertools import islice, cycle
from typing import Iterable

from dependency_injector.wiring import inject, Provide

from src import Config
from src.application.common import logger
from src.application.common.monitor_network import monitor_network
from src.application.contracts import ITileApiService, ITileService
from src.infra.infrastructure import Containers

TOTAL_REQUEST: int = 100_000
ZOOM_LEVEL: int = 13


@inject
def vector_tiles_100k_vmt(
        tile_service: ITileService = Provide[Containers.tile_service],
        tile_api_service: ITileApiService = Provide[Containers.tile_api_service],
) -> None:
    logger.info("Setting up benchmark")
    min_lat, min_lon, max_lat, max_lon = Config.BUILDINGS_SPATIAL_EXTENT
    existing_tiles: list[tuple[int, int, int]] = []
    tiles = tile_service.build_candidate_tiles(
        min_lat=min_lat,
        min_lon=min_lon,
        max_lat=max_lat,
        max_lon=max_lon,
        zoom=ZOOM_LEVEL
    )

    for test_tile in tiles:
        z, x, y = test_tile
        response = tile_api_service.fetch_vmt_tile(z=z, x=x, y=y)
        if response is not None:
            existing_tiles.append(test_tile)

    if not existing_tiles:
        raise RuntimeError("No candidate tiles found")

    print(existing_tiles)
    logger.info(f"Found {len(existing_tiles)} tiles. Repeating until {TOTAL_REQUEST} tiles are generated.")
    random.shuffle(tiles)
    tiles_to_request = islice(cycle(existing_tiles), TOTAL_REQUEST)

    _benchmark(tiles=tiles_to_request)


@inject
@monitor_network(query_id="vector-tiles-100k-vmt")
def _benchmark(
        tiles: Iterable[tuple[int, int, int]],
        tile_api_service: ITileApiService = Provide[Containers.tile_api_service],
) -> None:
    for z, x, y in tiles:
        tile_api_service.fetch_vmt_tile(z=z, x=x, y=y)
