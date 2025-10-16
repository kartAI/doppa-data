import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Config:
    IS_NOTEBOOK: bool = False

    # DIRECTORIES
    ROOT_DIR: Path = Path.cwd() if not IS_NOTEBOOK else Path.cwd().parent.parent.parent.parent
    DATASETS_PATH: Path = ROOT_DIR / "datasets"
    LOG_DIR: Path = ROOT_DIR / f"logs"
    TEMP_DIR: Path = DATASETS_PATH / "temp"

    # LOGGING
    LOGGING_LEVEL: int = logging.INFO
    LOG_FILE: Path = LOG_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # OPEN STREET MAP
    OSM_FILE_PATH: Path = DATASETS_PATH / "osm" / "norway-latest.osm.pbf"

    OSM_TEMP_PARQUET_DIR: Path = TEMP_DIR / "osm"

    OSM_BUILDINGS_GEOJSON_PATH: Path = DATASETS_PATH / "osm" / "buildings.geojson"
    OSM_BUILDINGS_PARQUET_PATH: Path = DATASETS_PATH / "osm" / "buildings.parquet"

    OSM_BUILDINGS_CLEANED_PARQUET_PATH: Path = DATASETS_PATH / "osm" / "cleaned_buildings.parquet"
    OSM_PBF_URL: str = "https://download.geofabrik.de/europe/norway-latest.osm.pbf"
    OSM_STREAMING_CHUNK_SIZE: int = 8192
    OSM_FEATURE_BATCH_SIZE: int = 250_000

    # FKB
    FKB_DIR: Path = DATASETS_PATH / "fkb"
    FKB_BUILDINGS_PARQUET_PATH: Path = FKB_DIR / "fkb_buildings.parquet"
    FKB_WGS84_BUILDINGS_PARQUET_PATH: Path = FKB_DIR / "fkb_wgs84_buildings.parquet"
