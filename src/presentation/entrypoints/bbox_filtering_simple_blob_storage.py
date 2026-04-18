import duckdb
from dependency_injector.wiring import inject, Provide

from src import Config
from src.application.common.monitor import monitor
from src.application.contracts import IFilePathService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, Theme, BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="bbox-filtering-simple-blob-storage",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_SIMPLE,
    cost_configuration=CostConfiguration(include_aci=True, include_blob_storage=True)
)
def bbox_filtering_simple_blob_storage(
        db_context: duckdb.DuckDBPyConnection = Provide[Containers.duckdb_context],
        file_path_service: IFilePathService = Provide[Containers.file_path_service]
) -> None:
    min_lon = 10.40
    max_lon = 10.95
    min_lat = 59.70
    max_lat = 60.10

    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        container=StorageContainer.DATA,
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet"
    )

    db_context.execute(
        f"""
            SELECT *, ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832')) AS area
            FROM read_parquet('{path}')
            WHERE ST_Intersects(
                geometry,
                ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat})
            )
            AND ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832')) > 10;
            """
    )
