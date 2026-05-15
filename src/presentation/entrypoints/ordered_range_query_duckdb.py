from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common.monitor import monitor
from src.application.contracts import IFilePathService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, Theme, BenchmarkIteration, BoundingBox, DatasetSize
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="ordered-range-query-duckdb",
    benchmark_iteration=BenchmarkIteration.ORDERED_RANGE_QUERY,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=True)
)
def ordered_range_query_duckdb(
        db_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        path_service: IFilePathService = Provide[Containers.file_path_service],
) -> list:
    path = path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        dataset_size=DatasetSize.SMALL,
        region="*",
        file_name="*.parquet",
    )

    min_lon, min_lat, max_lon, max_lat = BoundingBox.TRONDELAG_WGS84.value

    return db_context.execute(
        f"""
        SELECT * FROM read_parquet('{path}')
        WHERE ST_Intersects(
            geometry,
            ST_MakeEnvelope(?, ?, ?, ?)
        )
        ORDER BY building_id
        LIMIT 1000;
        """,
        [min_lon, min_lat, max_lon, max_lat],
    ).fetchall()
