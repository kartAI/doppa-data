from dependency_injector.wiring import inject, Provide

from src.application.common.monitor_network import monitor_network
from src.application.contracts import ITileApiService
from src.infra.infrastructure import Containers


@inject
@monitor_network(query_id="vector-tiles-single-tile-vmt")
def vector_tiles_single_tile_vmt(tile_api_service: ITileApiService = Provide[Containers.tile_api_service]) -> None:
    z, x, y = 13, 4340, 2382
    _ = tile_api_service.fetch_vmt_tile(z=z, x=x, y=y)
