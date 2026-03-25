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
    query_id="spatial-aggregation-h3-duckdb",
    benchmark_iteration=BenchmarkIteration.SPATIAL_AGGREGATION_H3,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=True)
)
def spatial_aggregation_h3_duckdb(
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

    h3_resolution = 7

    query = f"""
        WITH buildings AS (
            SELECT ST_Centroid(geometry) AS centroid
            FROM read_parquet('{path}')
            WHERE ST_IsValid(geometry)
        )
        SELECT
            h3_latlng_to_cell(
                ST_Y(centroid),
                ST_X(centroid),
                ?
            ) AS h3_cell,
            COUNT(*) AS building_count
        FROM buildings
        GROUP BY h3_cell
        ORDER BY building_count DESC;
    """

    db_context.execute(query, [h3_resolution])
