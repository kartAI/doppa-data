from dependency_injector.wiring import Provide, inject

from src.application.common.monitor_databricks import monitor_databricks
from src.application.contracts import IDatabricksService
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
def national_scale_spatial_join_databricks_4_nodes(
    databricks_service: IDatabricksService = Provide[Containers.databricks_service],
) -> None:
    _benchmark(databricks_service=databricks_service)


@inject
@monitor_databricks(
    query_id="national-scale-spatial-join-databricks-4-nodes",
    benchmark_iteration=BenchmarkIteration.NATIONAL_SCALE_SPATIAL_JOIN,
    cost_configuration=CostConfiguration(include_aci=True),
)
def _benchmark(
    databricks_service: IDatabricksService = Provide[Containers.databricks_service],
) -> None:
    databricks_service.submit_and_wait(num_workers=4)
