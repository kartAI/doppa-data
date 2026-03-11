from dependency_injector.wiring import inject, Provide

from src.application.common.monitor_network import monitor_network
from src.application.contracts import ITileApiService, ITileService
from src.infra.infrastructure import Containers

TOTAL_REQUESTS: int = 100_000


@inject
def vector_tiles_100k_vmt(tile_service: ITileService = Provide[Containers.tile_service]) -> None:
    tiles = tile_service.load_tiles(number_of_tiles=TOTAL_REQUESTS)
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
