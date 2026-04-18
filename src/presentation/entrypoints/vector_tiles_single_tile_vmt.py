from dependency_injector.wiring import inject, Provide

from src.application.common.monitor import monitor
from src.application.contracts import ITileApiService
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="vector-tiles-single-tile-vmt",
    benchmark_iteration=BenchmarkIteration.VECTOR_TILE_SINGLE_TILE,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def vector_tiles_single_tile_vmt(tile_api_service: ITileApiService = Provide[Containers.tile_api_service]) -> None:
    z, x, y = 13, 4340, 2382
    _ = tile_api_service.fetch_vmt_tile(z=z, x=x, y=y)
