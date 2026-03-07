import requests

from src import Config
from src.application.common.monitor_network import monitor_network

Z, X, Y = 13, 4340, 2382


@monitor_network(query_id="vector-tiles-single-tile-pmtiles")
def vector_tiles_single_tile_vmt() -> None:
    tile = requests.get(f"{Config.AZURE_VMT_SERVER_URL}/tiles/{Z}/{X}/{Y}")
    if tile is None:
        raise RuntimeError("Tile not found in archive")
