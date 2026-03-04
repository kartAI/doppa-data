from typing import Callable

import requests
from dependency_injector.wiring import inject, Provide
from pmtiles.reader import Reader

from src import Config
from src.application.common.monitor_network import monitor_network
from src.application.contracts import IFilePathService
from src.domain.enums import StorageContainer
from src.infra.infrastructure import Containers

Z, X, Y = 13, 4340, 2382


@inject
def vector_tiles_single_tile_pmtiles(
        file_path_service: IFilePathService = Provide[Containers.file_path_service]
) -> None:
    pmtiles_azure_url = file_path_service.create_url_to_blob_resource(
        container=StorageContainer.TILES,
        blob_path=Config.BUILDINGS_PMTILES_FILE.name
    )

    reader = Reader(_http_range_source(url=pmtiles_azure_url))
    _benchmark(reader=reader)


@monitor_network(query_id="vector-tiles-single-tile-pmtiles")
def _benchmark(reader: Reader) -> None:
    tile_bytes = reader.get(Z, X, Y)
    if tile_bytes is None:
        raise RuntimeError("Tile not found in archive")


def _http_range_source(url: str) -> Callable:
    session = requests.Session()

    def _get_bytes(offset: int, length: int) -> bytes:
        end = offset + length - 1
        headers = {
            "Range": f"bytes={offset}-{end}",
            "Accept-Encoding": "identity",
        }
        r = session.get(url, headers=headers, stream=False, timeout=30)
        if r.status_code != 206:
            if r.status_code != 200:
                r.raise_for_status()
            raise RuntimeError(f"Expected HTTP 206 Partial Content for range request, got {r.status_code}")

        content_range = r.headers.get("Content-Range")
        if content_range:
            try:
                units, range_spec = content_range.split(" ", 1)
                if units.strip().lower() != "bytes":
                    raise ValueError("Unsupported Content-Range units")
                byte_range, _ = range_spec.split("/", 1)
                start_str, end_str = byte_range.split("-", 1)
                start = int(start_str)
                end_returned = int(end_str)
            except Exception as exc:
                raise RuntimeError(f"Invalid Content-Range header: {content_range}") from exc
            if start != offset or (end_returned - start + 1) != length:
                raise RuntimeError(
                    f"Server returned unexpected byte range {content_range} "
                    f"for requested offset={offset}, length={length}"
                )

        if len(r.content) != length:
            raise RuntimeError(
                f"Server returned {len(r.content)} bytes, expected {length} "
                f"for offset={offset}"
            )
        return r.content

    return _get_bytes
