from dependency_injector import containers, providers
from pystac import StacIO

from src.infra.infrastructure.services import (
    BlobStorageService, OpenStreetMapService, OpenStreetMapFileService, FilePathService, ReleaseService, BytesService,
    CountyService, VectorService, StacService, StacIOService, FKBService, ZipService, FKBFileService
)
from src.infra.persistence.context import create_duckdb_context, create_blob_storage_context


class Containers(containers.DeclarativeContainer):
    config = providers.Configuration()
    db_context = providers.Singleton(create_duckdb_context)
    blob_storage_context = providers.Singleton(create_blob_storage_context)

    county_service = providers.Singleton(
        CountyService,
        db_context=db_context
    )

    file_path_service = providers.Singleton(
        FilePathService
    )

    bytes_service = providers.Singleton(
        BytesService
    )

    vector_service = providers.Singleton(
        VectorService,
        db_context=db_context
    )

    blob_storage_service = providers.Singleton(
        BlobStorageService,
        blob_storage_context=blob_storage_context,
        file_path_service=file_path_service
    )

    osm_file_service = providers.Singleton(
        OpenStreetMapFileService
    )

    stac_io_service = providers.Singleton(
        StacIOService,
        blob_storage_service=blob_storage_service
    )

    stac_service = providers.Singleton(
        StacService,
        blob_storage_service=blob_storage_service,
        file_path_service=file_path_service
    )

    release_service = providers.Singleton(
        ReleaseService,
        blob_storage_service=blob_storage_service,
        bytes_service=bytes_service
    )

    open_street_map_service = providers.Singleton(
        OpenStreetMapService,
        db_context=db_context,
        osm_file_service=osm_file_service,
        file_path_service=file_path_service,
        blob_storage_service=blob_storage_service,
        county_service=county_service,
        vector_service=vector_service,
        stac_service=stac_service
    )

    zip_service = providers.Singleton(
        ZipService,
    )

    fkb_file_service = providers.Singleton(
        FKBFileService,
    )

    fkb_service = providers.Singleton(
        FKBService,
        db_context=db_context,
        zip_service=zip_service,
        fkb_file_service=fkb_file_service,
        bytes_service=bytes_service,
        county_service=county_service,
        vector_service=vector_service,
        blob_storage_service=blob_storage_service
    )

    StacIO.set_default(stac_io_service)
