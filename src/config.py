import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


@dataclass(frozen=True)
class Config:
    IS_NOTEBOOK: bool = False
    IOU_ROUNDING_DECIMALS: int = 9

    # AZURE
    BLOB_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_BLOB_STORAGE_CONNECTION_STRING")
    BLOB_STORAGE_MAX_CONCURRENCY: int = 1

    # DIRECTORIES14:20:42,140
    ROOT_DIR: Path = Path.cwd() if not IS_NOTEBOOK else Path.cwd().parent.parent.parent
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
    OSM_COLUMNS_TO_KEEP: tuple[str, ...] = "id", "geometry", "building", "ref:bygningsnr"

    # FKB
    FKB_DIR: Path = DATASETS_PATH / "fkb"
    FKB_BUILDINGS_PARQUET_PATH: Path = FKB_DIR / "fkb_buildings.parquet"
    FKB_WGS84_BUILDINGS_PARQUET_PATH: Path = FKB_DIR / "fkb_wgs84_buildings.parquet"

    # GEONORGE
    GEONORGE_BASE_URL: str = "https://api.kartverket.no/kommuneinfo/v1/"

    # METADATA
    RELEASE_FILE_NAME: str = "releases.parquet"
    COUNTY_FILE_NAME: str = "counties.parquet"

    # STAC
    STAC_LICENSE = "CC-BY-4.0"
    STAC_STORAGE_CONTAINER = "https://doppablobstorage.blob.core.windows.net/stac"

    # FKB
    HUGGING_FACE_API_TOKEN: str = os.getenv("HUGGING_FACE_API_TOKEN")
    HUGGING_FACE_UTM32N_PATHS: tuple[str, ...] = (
        "https://huggingface.co/datasets/kartai/DX_datasett/resolve/main/Geodata/Bergen.zip",
        "https://huggingface.co/datasets/kartai/DX_datasett/resolve/main/Geodata/Kristiansand.zip",
        "https://huggingface.co/datasets/kartai/DX_datasett/resolve/main/Geodata/Sandvika.zip",
        "https://huggingface.co/datasets/kartai/DX_datasett/resolve/main/Geodata/Verdal.zip"
    )
    HUGGING_FACE_UTM33N_PATHS: tuple[str, ...] = (
        "https://huggingface.co/datasets/kartai/DX_datasett/resolve/main/Geodata/Mo_i_Rana.zip",
        "https://huggingface.co/datasets/kartai/DX_datasett/resolve/main/Geodata/Tromsdalen.zip"
    )
    FKB_LAYERS: tuple[str, ...] = (
        "Bygning", "AnnenBygning", "Takkant", "Bygningsdelelinje", "FiktivBygningsavgrensning"
    )

    # PARTITIONING
    PARTITION_RESOLUTION: int = 3
    OSM_FEATURE_BATCH_SIZE: int = 250_000  # TODO: Rename this to FEATURE_BATCH_SIZE
