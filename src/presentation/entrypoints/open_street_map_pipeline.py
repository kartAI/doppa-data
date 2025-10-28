from dependency_injector.wiring import inject, Provide
from pystac import CatalogType

from src import Config
from src.application.contracts import IOpenStreetMapService, IOpenStreetMapFileService, IReleaseService, IStacService
from src.infra.infrastructure import Containers


@inject
def extract_osm_buildings(
        osm_service: IOpenStreetMapService = Provide[Containers.open_street_map_service],
        osm_file_service: IOpenStreetMapFileService = Provide[Containers.osm_file_service],
        release_service: IReleaseService = Provide[Containers.release_service],
        stac_service: IStacService = Provide[Containers.stac_service]
) -> None:
    catalog = stac_service.get_catalog_root()
    release = release_service.create_release()
    release_catalog = stac_service.create_release_stac_catalog(root_catalog=catalog, release=release)

    osm_file_service.download_pbf()
    osm_service.process_osm_dataset(catalog=release_catalog, release=release)

    catalog.normalize_and_save(str(Config.DATASETS_PATH))
    # catalog.normalize_hrefs("https://doppablobstorage.blob.core.windows.net/stac/")
    catalog.save(CatalogType.ABSOLUTE_PUBLISHED)
