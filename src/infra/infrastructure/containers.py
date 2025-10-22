from dependency_injector import containers, providers

from src.infra.infrastructure.services import (
    BlobStorageService, OpenStreetMapService, OpenStreetMapFileService, FilePathService
)
from src.infra.persistence.context import create_duckdb_context, create_blob_storage_context


class Containers(containers.DeclarativeContainer):
    config = providers.Configuration()
    db_context = providers.Singleton(create_duckdb_context)
    blob_storage_context = providers.Singleton(create_blob_storage_context)

    file_path_service = providers.Singleton(
        FilePathService
    )

    blob_storage_service = providers.Singleton(
        BlobStorageService,
        blob_storage_context=blob_storage_context
    )

    osm_file_service = providers.Singleton(
        OpenStreetMapFileService
    )

    open_street_map_service = providers.Singleton(
        OpenStreetMapService,
        db_context=db_context,
        osm_file_service=osm_file_service
    )
