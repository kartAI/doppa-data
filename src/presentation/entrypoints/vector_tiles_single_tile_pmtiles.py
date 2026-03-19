from dependency_injector.wiring import inject, Provide
from pmtiles.reader import Reader

from src import Config
from src.application.common.monitor_network import monitor_network
from src.application.contracts import IFilePathService, ITileApiService
from src.domain.enums import StorageContainer
from src.infra.infrastructure import Containers

Z, X, Y = 13, 4340, 2382


@inject
def vector_tiles_single_tile_pmtiles(
        file_path_service: IFilePathService = Provide[Containers.file_path_service],
        tile_api_service: ITileApiService = Provide[Containers.tile_api_service]
) -> None:
    pmtiles_azure_url = file_path_service.create_url_to_blob_resource(
        container=StorageContainer.TILES,
        blob_path=Config.BUILDINGS_PMTILES_FILE.name
    )

    reader = tile_api_service.create_pmtiles_reader(pmtiles_url=pmtiles_azure_url)
    _benchmark(reader=reader)


@monitor_network(query_id="vector-tiles-single-tile-pmtiles", benchmark_iterations=600)
def _benchmark(reader: Reader, tile_api_service: ITileApiService = Provide[Containers.tile_api_service]) -> None:
    tile_bytes = tile_api_service.fetch_pmtiles_tile(reader=reader, z=Z, x=X, y=Y)
    if tile_bytes is None:
        raise RuntimeError("Tile not found in archive")
