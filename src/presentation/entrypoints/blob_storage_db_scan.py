from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src.application.contracts import IFilePathService
from src.domain.enums import StorageContainer, Theme
from src.infra.infrastructure import Containers


@inject
def blob_storage_db_scan(
        db_context: DuckDBPyConnection = Provide[Containers.db_context],
        path_service: IFilePathService = Provide[Containers.file_path_service]
) -> None:
    path = path_service.create_virtual_filesystem_path(
        storage_scheme="az",
        release="2026-02-16.3",
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet"
    )

    db_context.execute(f"SELECT count(*) AS count FROM read_parquet('{path}')")
