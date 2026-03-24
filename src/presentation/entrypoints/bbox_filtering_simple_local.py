import duckdb
from dependency_injector.wiring import Provide, inject

from src import Config
from src.application.common.monitor_cpu_and_ram import monitor_cpu_and_ram
from src.application.contracts import IFilePathService, IBenchmarkService
from src.domain.enums import StorageContainer, Theme, BenchmarkIteration
from src.infra.infrastructure import Containers


def bbox_filtering_simple_local() -> None:
    _download_data()
    _benchmark()


@inject
@monitor_cpu_and_ram(
    query_id="bbox-filtering-simple-local",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_SIMPLE
)
def _benchmark(db_context: duckdb.DuckDBPyConnection = Provide[Containers.duckdb_context]) -> None:
    min_lon = 10.40
    max_lon = 10.95
    min_lat = 59.70
    max_lat = 60.10

    db_context.execute(
        f"""
        SELECT * FROM ST_ReadShp('{str(Config.BUILDINGS_SHAPEFILE)}')
        WHERE ST_Intersects(
            geom,
            ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat})
        );
        """
    )


@inject
def _download_data(
        file_path_service: IFilePathService = Provide[Containers.file_path_service],
        benchmark_service: IBenchmarkService = Provide[Containers.benchmark_service]
) -> None:
    virtual_file_path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        container=StorageContainer.DATA,
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet"
    )

    benchmark_service.download_parquet_as_shapefile_locally(
        virtual_file_path=virtual_file_path,
        save_path=Config.BUILDINGS_SHAPEFILE
    )
