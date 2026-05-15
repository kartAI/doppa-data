from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common.monitor import monitor
from src.application.contracts import IFilePathService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, Theme, BenchmarkIteration, DatasetSize
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="national-scale-spatial-join-duckdb",
    benchmark_iteration=BenchmarkIteration.NATIONAL_SCALE_SPATIAL_JOIN,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=True),
)
def national_scale_spatial_join_duckdb(
    db_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
    path_service: IFilePathService = Provide[Containers.file_path_service],
) -> list:
    buildings_path = path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        dataset_size=DatasetSize.SMALL,
        region="*",
        file_name="*.parquet",
    )
    counties_path = f"az://{StorageContainer.METADATA.value}/{Config.DATABRICKS_MUNICIPALITIES_FILE}"

    return db_context.execute(f"""
        WITH counties AS (
            SELECT
                region AS county_name,
                ST_GeomFromWKB(wkb) AS geometry
            FROM read_parquet('{counties_path}')
        )
        SELECT
            c.county_name,
            COUNT(*) AS building_count
        FROM counties c
        JOIN read_parquet('{buildings_path}') b
          ON ST_Intersects(c.geometry, b.geometry)
        GROUP BY c.county_name
        ORDER BY building_count DESC
    """).fetchall()
