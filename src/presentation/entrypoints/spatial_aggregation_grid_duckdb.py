from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common.monitor_network import monitor_network
from src.application.contracts import IFilePathService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, Theme, BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor_network(
    query_id="spatial-aggregation-grid-duckdb",
    benchmark_iteration=BenchmarkIteration.SPATIAL_AGGREGATION_GRID,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=True)
)
def spatial_aggregation_grid_duckdb(
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

    cell_size = 0.01

    query = f"""
        WITH buildings AS (
            SELECT ST_Centroid(geometry) AS centroid
            FROM read_parquet('{path}')
            WHERE ST_IsValid(geometry)
        )
        SELECT
            FLOOR(ST_Y(centroid) / ?) AS lat_cell,
            FLOOR(ST_X(centroid) / ?) AS lng_cell,
            COUNT(*) AS building_count
        FROM buildings
        GROUP BY lat_cell, lng_cell
        ORDER BY building_count DESC;
    """

    db_context.execute(query, [cell_size, cell_size])
