from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common.monitor import monitor_cpu_and_ram
from src.application.contracts import IFilePathService
from src.domain.enums import StorageContainer, Theme
from src.infra.infrastructure import Containers


@inject
@monitor_cpu_and_ram(query_id="db-scan-blob-storage")
def db_scan_blob_storage(
        db_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        path_service: IFilePathService = Provide[Containers.file_path_service]
) -> None:
    path = path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet"
    )

    db_context.execute(f"SELECT count(*) AS count FROM read_parquet('{path}')")
