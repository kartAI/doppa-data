from dependency_injector.wiring import inject, Provide

from src.application.contracts import IOpenStreetMapService, IOpenStreetMapFileService, IReleaseService, IStacService, \
    IFKBService
from src.infra.infrastructure import Containers


@inject
def extract_osm_buildings(
        osm_service: IOpenStreetMapService = Provide[Containers.open_street_map_service],
        osm_file_service: IOpenStreetMapFileService = Provide[Containers.osm_file_service],
        release_service: IReleaseService = Provide[Containers.release_service],
        stac_service: IStacService = Provide[Containers.stac_service],
        fkb_service: IFKBService = Provide[Containers.fkb_service]
) -> None:
    catalog = stac_service.get_catalog_root()
    release = release_service.create_release()
    release_catalog = stac_service.create_release_catalog(root_catalog=catalog, release=release)

    fkb_service.process_fkb_dataset(catalog, release)
    exit()

    osm_file_service.download_pbf()
    osm_service.process_osm_dataset(catalog=release_catalog, release=release)

    stac_service.save_catalog(catalog)
