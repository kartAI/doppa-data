import requests
from dependency_injector.wiring import inject, Provide
from pmtiles.reader import Reader

from src import Config
from src.application.common.monitor_network import monitor_network
from src.application.contracts import IFilePathService
from src.domain.enums import StorageContainer
from src.infra.infrastructure import Containers

Z, X, Y = 15, 17361, 9529


@inject
def vector_tiles_pmtiles(file_path_service: IFilePathService = Provide[Containers.file_path_service]) -> None:
    pmtiles_azure_path = file_path_service.create_url_to_blob_resource(
        container=StorageContainer.TILES,
        blob_path=Config.BUILDINGS_PMTILES_FILE.name
    )

    response = requests.get(pmtiles_azure_path, stream=True)
    response.raise_for_status()

    reader = Reader(response.raw)
    _benchmark(reader=reader)


@monitor_network(query_id="vector-tiles-pmtiles")
def _benchmark(reader: Reader) -> None:
    reader.get(Z, X, Y)
