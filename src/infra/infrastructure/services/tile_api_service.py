from requests import Request, Session, session, RequestException

from src import Config
from src.application.contracts import ITileApiService


class TileApiService(ITileApiService):
    __session: Session

    def __init__(self):
        self.__session = session()

    def fetch_vmt_tile(self, z: int, x: int, y: int) -> bytes:
        try:
            tile_response = self.__session.get(
                f"{Config.AZURE_VMT_SERVER_URL}/tiles/{z}/{x}/{y}",
                timeout=10,
                headers={
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache, no-store, max-age=0"
                }
            )
            tile_response.raise_for_status()

        except RequestException as e:
            raise RuntimeError("Failed to fetch tile from VMT server") from e

        if not tile_response.content:
            raise RuntimeError("Tile not found on VMT server")

        return tile_response.content
