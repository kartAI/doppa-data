from requests import Session, session, RequestException

from src import Config
from src.application.contracts import ITileApiService, ITileService


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
