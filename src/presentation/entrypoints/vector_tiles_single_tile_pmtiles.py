from dependency_injector.wiring import inject, Provide
from pmtiles.reader import Reader

from src import Config
from src.application.common.monitor import monitor
from src.application.contracts import IFilePathService, ITileApiService
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, BenchmarkIteration
from src.infra.infrastructure import Containers

Z, X, Y = 13, 4340, 2382


@inject
def vector_tiles_single_tile_pmtiles(
        file_path_service: IFilePathService = Provide[Containers.file_path_service],
        tile_api_service: ITileApiService = Provide[Containers.tile_api_service]
) -> None:
    """
    Benchmark: single vector tile fetch (z=13, x=4340, y=2382) from the buildings
    PMTiles archive on Azure Blob Storage. Opens a PMTiles reader against the blob
    URL before timing the single tile read.
    """
    pmtiles_azure_url = file_path_service.create_url_to_blob_resource(
        container=StorageContainer.TILES,
        blob_path=Config.BUILDINGS_PMTILES_FILE.name
    )

    reader = tile_api_service.create_pmtiles_reader(pmtiles_url=pmtiles_azure_url)
    _benchmark(reader=reader)


@monitor(
    query_id="vector-tiles-single-tile-pmtiles",
    benchmark_iteration=BenchmarkIteration.VECTOR_TILE_SINGLE_TILE,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def _benchmark(reader: Reader, tile_api_service: ITileApiService = Provide[Containers.tile_api_service]) -> None:
    tile_bytes = tile_api_service.fetch_pmtiles_tile(reader=reader, z=Z, x=X, y=Y)
    if tile_bytes is None:
        raise RuntimeError("Tile not found in archive")
