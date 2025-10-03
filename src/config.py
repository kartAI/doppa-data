import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Config:
    IS_NOTEBOOK: bool = True

    # DIRECTORIES
    ROOT_DIR: Path = Path.cwd() if not IS_NOTEBOOK else Path.cwd().parent.parent.parent.parent
    DATASETS_PATH: Path = ROOT_DIR / "datasets"
    LOG_DIR: Path = ROOT_DIR / f"logs"

    # LOGGING
    LOGGING_LEVEL: int = logging.INFO
    LOG_FILE: Path = LOG_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # OPEN STREET MAP
    OSM_FILE_PATH: Path = DATASETS_PATH / "osm" / "norway-latest.osm.pbf"
    OSM_BUILDINGS_GEOJSON_PATH: Path = DATASETS_PATH / "osm" / "buildings.geojson"
    OSM_PBF_URL: str = "https://download.geofabrik.de/europe/norway-latest.osm.pbf"
    OSM_STREAMING_CHUNK_SIZE: int = 8192
