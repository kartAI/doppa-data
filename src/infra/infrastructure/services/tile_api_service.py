from typing import Callable

from pmtiles.reader import Reader
from requests import Session, session, RequestException

from src import Config
from src.application.contracts import ITileApiService


class TileApiService(ITileApiService):
    __session: Session

    def __init__(self):
        self.__session = session()

    def fetch_vmt_tile(self, z: int, x: int, y: int) -> bytes | None:
        try:
            tile_response = self.__session.get(
                f"{Config.AZURE_VMT_SERVER_URL}/tiles/{z}/{x}/{y}",
                timeout=10,
                headers={
                    "Cache-Control": "no-cache, no-store, max-age=0",
                    "Pragma": "no-cache"
                }
            )
        except RequestException as e:
            raise RuntimeError("Failed to fetch tile from VMT server") from e

        if tile_response.status_code == 404:
            return None

        try:
            tile_response.raise_for_status()
        except RequestException as e:
            raise RuntimeError("Failed to fetch tile from VMT server") from e

        if not tile_response.content:
            return None

        return tile_response.content

    def fetch_pmtiles_tile(self, reader: Reader, z: int, x: int, y: int) -> bytes | None:
        return reader.get(z, x, y)

    def create_pmtiles_reader(self, pmtiles_url: str) -> Reader:
        return Reader(self.__http_range_source(url=pmtiles_url))

    def __http_range_source(self, url: str) -> Callable:
        def _get_bytes(offset: int, length: int) -> bytes:
            end = offset + length - 1
            headers = {
                "Range": f"bytes={offset}-{end}",
                "Accept-Encoding": "identity",
            }
            r = self.__session.get(url, headers=headers, stream=False, timeout=30)
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
