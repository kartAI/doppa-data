import requests

from src import Config
from src.application.common.monitor_network import monitor_network

Z, X, Y = 13, 4340, 2382

session = requests.session()


@monitor_network(query_id="vector-tiles-single-tile-vmt")
def vector_tiles_single_tile_vmt() -> None:
    try:
        tile_response = session.get(
            f"{Config.AZURE_VMT_SERVER_URL}/tiles/{Z}/{X}/{Y}",
            timeout=10,
        )
        tile_response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError("Failed to fetch tile from VMT server") from exc
    if not tile_response.content:
        raise RuntimeError("Tile not found on VMT server")
