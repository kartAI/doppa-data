import subprocess

import geopandas as gpd
from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection
from shapely import from_wkb
from sqlalchemy import Engine

from src import Config
from src.application.common import logger
from src.application.contracts import IFilePathService, IBlobStorageService, IBytesService
from src.domain.enums import StorageContainer, Theme, EPSGCode
from src.infra.infrastructure import Containers


def setup_benchmarking_framework() -> None:
    _postgres_buildings_seed()
    _create_pmtiles()
    _create_mvt()


@inject
def _postgres_buildings_seed(
        duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        postgres_db_context: Engine = Provide[Containers.postgres_context],
        file_path_service: IFilePathService = Provide[Containers.file_path_service],
) -> None:
    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet"
    )

    logger.info(f"Fetching buildings from '{path}'")
    building_df = duckdb_context.execute(
        f"SELECT ST_AsWKB(geometry) AS geometry, * EXCLUDE geometry FROM read_parquet('{path}')").fetchdf()

    building_df["geometry"] = building_df["geometry"].apply(
        lambda g: bytes(g) if isinstance(g, (memoryview, bytearray)) else g
    )

    building_df["geometry"] = building_df["geometry"].apply(from_wkb)

    building_gdf = gpd.GeoDataFrame(
        building_df,
        geometry="geometry",
        crs=EPSGCode.WGS84.value,
    )

    if building_gdf.empty:
        logger.warning(f"No buildings found at blob storage path '{path}'")
        return

    total_rows = building_gdf.shape[0]
    logger.info(f"Inserting {total_rows} rows into 'buildings' table in chunks of {Config.BUILDINGS_BATCH_SIZE}...")

    with postgres_db_context.connect() as conn:
        for i in range(0, total_rows, Config.BUILDINGS_BATCH_SIZE):
            chunk = building_gdf.iloc[i:i + Config.BUILDINGS_BATCH_SIZE]
            chunk.to_postgis(
                name="buildings",
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False
            )
            logger.info(f"Inserted rows {i + 1} to {min(i + Config.BUILDINGS_BATCH_SIZE, total_rows)} of {total_rows}")

    logger.info("Insertion completed")


@inject
def _create_pmtiles(
        duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        file_path_service: IFilePathService = Provide[Containers.file_path_service],
        blob_storage_service: IBlobStorageService = Provide[Containers.blob_storage_service],
        bytes_service: IBytesService = Provide[Containers.bytes_service]

) -> None:
    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet",
    )

    Config.BUILDINGS_GEOJSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
    Config.BUILDINGS_PMTILES_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching buildings as GeoJSONL file.")

    duckdb_context.execute(
        f"""
        COPY (
            SELECT 
                * EXCLUDE (geometry, bbox),
                bbox.maxx, 
                bbox.maxy, 
                bbox.minx, 
                bbox.miny,
                geometry
            FROM read_parquet('{path}')
        )
        TO '{Config.BUILDINGS_GEOJSONL_FILE.as_posix()}'
        WITH (
            FORMAT GDAL,
            DRIVER 'GeoJSONSeq'
        );
        """
    )

    logger.info(f"Saved buildings to '{Config.BUILDINGS_GEOJSONL_FILE}'")

    cmd = [
        "tippecanoe",
        "-o",
        Config.BUILDINGS_PMTILES_FILE.as_posix(),
        "-zg",
        "--drop-densest-as-needed",
        "--coalesce",
        "--read-parallel",
        "-l",
        "buildings",
        Config.BUILDINGS_GEOJSONL_FILE.as_posix(),
    ]

    cmd_str = " ".join(cmd)
    logger.info(f"Creating PMTiles with command:\t{cmd_str}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"tippecanoe failed with exit code {result.returncode}:\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    logger.info(f"PMTiles saved to '{Config.BUILDINGS_PMTILES_FILE}'")
    logger.info("Uploading PMTiles to blob storage.")

    pmtiles_bytes = bytes_service.convert_pmtiles_to_bytes(Config.BUILDINGS_PMTILES_FILE)
    blob_storage_service.upload_file(
        container_name=StorageContainer.TILES,
        blob_name=Config.BUILDINGS_PMTILES_FILE.name,
        data=pmtiles_bytes
    )

    logger.info(f"Uploaded PMTiles to container '{StorageContainer.TILES.value}'")


@inject
def _create_mvt(
        duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        file_path_service: IFilePathService = Provide[Containers.file_path_service],
        blob_storage_service: IBlobStorageService = Provide[Containers.blob_storage_service],
) -> None:
    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet",
    )

    Config.BUILDINGS_GEOJSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
    Config.BUILDINGS_MVT_DIR.parent.mkdir(parents=True, exist_ok=True)

    if not Config.BUILDINGS_GEOJSONL_FILE.exists():
        logger.info("Fetching buildings as GeoJSONL file for MVT generation.")

        duckdb_context.execute(
            f"""
            COPY (
                SELECT 
                    * EXCLUDE (geometry, bbox),
                    bbox.maxx, 
                    bbox.maxy, 
                    bbox.minx, 
                    bbox.miny,
                    geometry
                FROM read_parquet('{path}')
            )
            TO '{Config.BUILDINGS_GEOJSONL_FILE.as_posix()}'
            WITH (
                FORMAT GDAL,
                DRIVER 'GeoJSONSeq'
            );
            """
        )

        logger.info(f"Saved buildings to '{Config.BUILDINGS_GEOJSONL_FILE}'")
    else:
        logger.info(f"GeoJSONL file already exists at '{Config.BUILDINGS_GEOJSONL_FILE}', skipping creation.")

    cmd = [
        "tippecanoe",
        "-e",
        Config.BUILDINGS_MVT_DIR.as_posix(),
        "-zg",
        "--drop-densest-as-needed",
        "--coalesce",
        "--read-parallel",
        "--no-tile-compression",
        "-l",
        "buildings",
        Config.BUILDINGS_GEOJSONL_FILE.as_posix(),
    ]

    cmd_str = " ".join(cmd)
    logger.info(f"Creating MVT tiles with command:\t{cmd_str}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"tippecanoe failed with exit code {result.returncode}:\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    logger.info(f"MVT tiles saved to '{Config.BUILDINGS_MVT_DIR}'")
    logger.info("Uploading MVT tiles to blob storage.")

    mvt_dir = Config.BUILDINGS_MVT_DIR
    tile_count = 0

    for tile_file in mvt_dir.rglob("*.pbf"):
        relative_path = tile_file.relative_to(mvt_dir)
        blob_name = f"mvt/{relative_path.as_posix()}"

        tile_bytes = tile_file.read_bytes()
        blob_storage_service.upload_file(
            container_name=StorageContainer.TILES,
            blob_name=blob_name,
            data=tile_bytes,
        )
        tile_count += 1

    logger.info(f"Uploaded {tile_count} MVT tiles to container '{StorageContainer.TILES.value}' under 'mvt/' prefix.")

