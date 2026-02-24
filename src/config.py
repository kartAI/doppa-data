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
    AZURE_RESOURCE_GROUP: str = "doppa"
    AZURE_RESOURCE_LOCATION: str = "norwayeast"
    AZURE_BLOB_STORAGE_ACCOUNT_NAME: str = "doppablobstorage"
    BLOB_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_BLOB_STORAGE_CONNECTION_STRING")
    BLOB_STORAGE_MAX_CONCURRENCY: int = 1

    # DIRECTORIES14:20:42,140
    ROOT_DIR: Path = Path.cwd() if not IS_NOTEBOOK else Path.cwd().parent.parent.parent
    LOG_DIR: Path = ROOT_DIR / f"logs"

    # LOGGING
    LOGGING_LEVEL: int = logging.INFO
    LOG_FILE: Path = LOG_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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

    # BENCHMARKING
    BENCHMARK_FILE: Path = ROOT_DIR / "benchmarks.yml"
    RUN_ID_LENGTH: int = 6
    DEFAULT_SAMPLE_TIMEOUT: float = 0.00005
    BENCHMARK_WARMUP_RUNS: int = 3
    BENCHMARK_RUNS: int = 30
    BENCHMARK_METADATA_BLOB_NAME: str = "benchmark_metadata.parquet"
