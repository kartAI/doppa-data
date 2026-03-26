from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common.monitor_network import monitor_network
from src.application.contracts import IFilePathService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, Theme, BenchmarkIteration, BoundingBox
from src.infra.infrastructure import Containers


def bbox_filtering_result_set_sizes_neighborhood_duckdb() -> None:
    _benchmark()


@inject
@monitor_network(
    query_id="bbox-filtering-result-set-sizes-neighborhood-duckdb",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_RESULT_SET_SIZES,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=True)
)
def _benchmark(
        db_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        path_service: IFilePathService = Provide[Containers.file_path_service],
) -> None:
    path = path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet",
    )

    min_lon, min_lat, max_lon, max_lat = BoundingBox.NEIGHBORHOOD_WGS84.value

    db_context.execute(
        f"""
        SELECT * FROM read_parquet('{path}')
        WHERE ST_Intersects(
            geometry,
            ST_MakeEnvelope(?, ?, ?, ?)
        );
        """,
        [min_lon, min_lat, max_lon, max_lat],
    )
