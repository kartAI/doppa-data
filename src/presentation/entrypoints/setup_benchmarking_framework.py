import json
import subprocess

import geopandas as gpd
from osgeo import ogr
from pyproj import CRS
from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection
from shapely import from_wkb
from sqlalchemy import Engine, text

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IFilePathService,
    IBlobStorageService,
    IBytesService,
    ITileService,
    ITileApiService,
    ITestDatasetService,
    IDatasetSynthesisService,
    IBenchmarkService,
)
from src.domain.enums import StorageContainer, Theme, EPSGCode, DatasetSize
from src.infra.infrastructure import Containers


@inject
def setup_benchmarking_framework(
    test_dataset_service: ITestDatasetService = Provide[
        Containers.test_dataset_service
    ],
    dataset_synthesis_service: IDatasetSynthesisService = Provide[
        Containers.dataset_synthesis_service
    ],
) -> None:
    logger.info("Starting benchmarking framework setup...")

    logger.info("Step 1/5: Running test dataset pipeline...")
    release = test_dataset_service.run_pipeline()
    logger.info(f"Test dataset pipeline complete. Release: '{release}'")

    logger.info("Step 2/5: Synthesizing medium dataset...")
    dataset_synthesis_service.run_pipeline(release=release, target_size=DatasetSize.MEDIUM)
    logger.info("Medium dataset synthesis complete.")

    logger.info("Step 3/5: Synthesizing large dataset...")
    dataset_synthesis_service.run_pipeline(release=release, target_size=DatasetSize.LARGE)
    logger.info("Large dataset synthesis complete.")

    logger.info("Step 4/5: Seeding Postgres with buildings...")
    _postgres_buildings_seed(release=release)
    logger.info("Postgres seed complete.")

    logger.info("Step 5/5: Creating shapefile copy in blob storage...")
    _create_shapefile_copy(release=release)
    logger.info("Shapefile copy complete.")

    logger.info("Benchmarking framework setup complete.")


@inject
def _postgres_buildings_seed(
    release: str | None = None,
    duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
    postgres_db_context: Engine = Provide[Containers.postgres_context],
    file_path_service: IFilePathService = Provide[Containers.file_path_service],
) -> None:
    effective_release = release or Config.BENCHMARK_DOPPA_DATA_RELEASE

    for size in DatasetSize:
        _seed_postgres_for_size(
            release=effective_release,
            size=size,
            duckdb_context=duckdb_context,
            postgres_db_context=postgres_db_context,
            file_path_service=file_path_service,
        )


def _seed_postgres_for_size(
    release: str,
    size: DatasetSize,
    duckdb_context: DuckDBPyConnection,
    postgres_db_context: Engine,
    file_path_service: IFilePathService,
) -> None:
    table_name = f"buildings_{size.value}"
    index_name = f"{table_name}_geometry_idx"

    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=release,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        dataset_size=size,
        region="*",
        file_name="*.parquet",
    )

    logger.info(f"Fetching buildings for size '{size.value}' from '{path}'")
    building_df = duckdb_context.execute(
        f"SELECT ST_AsWKB(geometry) AS geometry, * EXCLUDE geometry FROM read_parquet('{path}')"
    ).fetchdf()

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
        logger.warning(
            f"No buildings found at blob storage path '{path}' for size '{size.value}'. Skipping table '{table_name}'."
        )
        return

    total_rows = building_gdf.shape[0]
    logger.info(
        f"Inserting {total_rows} rows into '{table_name}' in chunks of {Config.BUILDINGS_BATCH_SIZE}..."
    )

    with postgres_db_context.connect() as conn:
        for i in range(0, total_rows, Config.BUILDINGS_BATCH_SIZE):
            chunk = building_gdf.iloc[i : i + Config.BUILDINGS_BATCH_SIZE]
            chunk.to_postgis(
                name=table_name,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )
            logger.info(
                f"Inserted rows {i + 1} to {min(i + Config.BUILDINGS_BATCH_SIZE, total_rows)} of {total_rows} into '{table_name}'"
            )

        logger.info(f"Creating GIST spatial index '{index_name}' on '{table_name}.geometry'...")
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST (geometry)"
            )
        )
        conn.commit()
        logger.info(f"Spatial index '{index_name}' created.")

    logger.info(f"Insertion into '{table_name}' completed")


@inject
def _create_pmtiles(
    release: str | None = None,
    duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
    file_path_service: IFilePathService = Provide[Containers.file_path_service],
    blob_storage_service: IBlobStorageService = Provide[
        Containers.blob_storage_service
    ],
    bytes_service: IBytesService = Provide[Containers.bytes_service],
) -> None:
    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=release or Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        dataset_size=DatasetSize.SMALL,
        region="*",
        file_name="*.parquet",
    )

    Config.BUILDINGS_GEOJSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
    Config.BUILDINGS_PMTILES_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching buildings as GeoJSONL file.")

    duckdb_context.execute(f"""
        COPY (
            SELECT
                * EXCLUDE (geometry, bbox),
                ST_XMax(geometry) AS bbox_maxx,
                ST_YMax(geometry) AS bbox_maxy,
                ST_XMin(geometry) AS bbox_minx,
                ST_YMin(geometry) AS bbox_miny,
                geometry
            FROM read_parquet('{path}')
        )
        TO '{Config.BUILDINGS_GEOJSONL_FILE.as_posix()}'
        WITH (
            FORMAT GDAL,
            DRIVER 'GeoJSONSeq'
        );
        """)

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

    logger.info("Running tippecanoe to generate PMTiles (this may take a while)...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"tippecanoe failed with exit code {result.returncode}:\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    logger.info(f"PMTiles saved to '{Config.BUILDINGS_PMTILES_FILE}'")
    logger.info("Uploading PMTiles to blob storage.")

    pmtiles_bytes = bytes_service.convert_pmtiles_to_bytes(
        Config.BUILDINGS_PMTILES_FILE
    )
    blob_storage_service.upload_file(
        container_name=StorageContainer.TILES,
        blob_name=Config.BUILDINGS_PMTILES_FILE.name,
        data=pmtiles_bytes,
    )

    logger.info(f"Uploaded PMTiles to container '{StorageContainer.TILES.value}'")


@inject
def _create_mvt(
    release: str | None = None,
    duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
    file_path_service: IFilePathService = Provide[Containers.file_path_service],
    blob_storage_service: IBlobStorageService = Provide[
        Containers.blob_storage_service
    ],
) -> None:
    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=release or Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        dataset_size=DatasetSize.SMALL,
        region="*",
        file_name="*.parquet",
    )

    Config.BUILDINGS_GEOJSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
    Config.BUILDINGS_MVT_DIR.parent.mkdir(parents=True, exist_ok=True)

    if not Config.BUILDINGS_GEOJSONL_FILE.exists():
        logger.info("Fetching buildings as GeoJSONL file for MVT generation.")

        duckdb_context.execute(f"""
            COPY (
                SELECT
                    * EXCLUDE (geometry, bbox),
                    ST_XMax(geometry) AS bbox_maxx,
                    ST_YMax(geometry) AS bbox_maxy,
                    ST_XMin(geometry) AS bbox_minx,
                    ST_YMin(geometry) AS bbox_miny,
                    geometry
                FROM read_parquet('{path}')
            )
            TO '{Config.BUILDINGS_GEOJSONL_FILE.as_posix()}'
            WITH (
                FORMAT GDAL,
                DRIVER 'GeoJSONSeq'
            );
            """)

        logger.info(f"Saved buildings to '{Config.BUILDINGS_GEOJSONL_FILE}'")
    else:
        logger.info(
            f"GeoJSONL file already exists at '{Config.BUILDINGS_GEOJSONL_FILE}', skipping creation."
        )

    cmd = [
        "tippecanoe",
        "-e",
        Config.BUILDINGS_MVT_DIR.as_posix(),
        "-zg",
        "--drop-densest-as-needed",
        "--coalesce",
        "--read-parallel",
        "--no-tile-compression",
        "--force",
        "-l",
        "buildings",
        Config.BUILDINGS_GEOJSONL_FILE.as_posix(),
    ]

    logger.info("Running tippecanoe to generate MVT tiles (this may take a while)...")
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

    logger.info(
        f"Uploaded {tile_count} MVT tiles to container '{StorageContainer.TILES.value}' under 'mvt/' prefix."
    )


@inject
def _generate_tiles_file(
    tile_service: ITileService = Provide[Containers.tile_service],
    tile_api_service: ITileApiService = Provide[Containers.tile_api_service],
) -> None:
    TILE_ZOOM: int = 13

    min_lat, min_lon, max_lat, max_lon = Config.BUILDINGS_SPATIAL_EXTENT
    candidate_tiles = tile_service.build_candidate_tiles(
        min_lat=min_lat,
        min_lon=min_lon,
        max_lat=max_lat,
        max_lon=max_lon,
        zoom=TILE_ZOOM,
    )

    logger.info(f"Created {len(candidate_tiles)} candidate tiles")
    logger.info("Finding candidate tiles with data...")

    existing_tiles: list[tuple[int, int, int]] = []
    for candidate_tile in candidate_tiles:
        z, x, y = candidate_tile
        if tile_api_service.fetch_vmt_tile(z=z, x=x, y=y) is not None:
            existing_tiles.append(candidate_tile)

    logger.info(
        f"Found {len(existing_tiles)} tiles with data out of {len(candidate_tiles)} candidates"
    )

    with open(Config.MVT_TILES_PATH, "w", encoding="utf-8") as f:
        json.dump([list(tile) for tile in existing_tiles], f)

    logger.info(f"Tiles file saved to '{Config.MVT_TILES_PATH}'")


@inject
def _create_shapefile_copy(
    release: str | None = None,
    file_path_service: IFilePathService = Provide[Containers.file_path_service],
    benchmark_service: IBenchmarkService = Provide[Containers.benchmark_service],
    blob_storage_service: IBlobStorageService = Provide[
        Containers.blob_storage_service
    ],
) -> None:

    path = file_path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release=release or Config.BENCHMARK_DOPPA_DATA_RELEASE,
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        dataset_size=DatasetSize.SMALL,
        region="*",
        file_name="*.parquet",
    )

    logger.info("Converting parquet to shapefile locally...")
    benchmark_service.download_parquet_as_shapefile_locally(
        virtual_file_path=path,
        save_path=Config.BUILDINGS_SHAPEFILE,
    )
    logger.info(f"Shapefile written to '{Config.BUILDINGS_SHAPEFILE}'")

    prj_path = Config.BUILDINGS_SHAPEFILE.with_suffix(".prj")
    prj_path.write_text(CRS.from_epsg(4326).to_wkt(version="WKT1_ESRI"))
    logger.info(f"WGS84 .prj file written to '{prj_path}'")

    logger.info("Creating spatial index (.qix)...")
    ogr.UseExceptions()
    ds = ogr.Open(Config.BUILDINGS_SHAPEFILE.as_posix(), update=1)
    ds.ExecuteSQL(f"CREATE SPATIAL INDEX ON {Config.BUILDINGS_SHAPEFILE.stem}")
    ds = None
    logger.info("Spatial index created.")

    blob_prefix = "copies/shapefile"
    base = Config.BUILDINGS_SHAPEFILE.with_suffix("")
    for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix"):
        component = base.with_suffix(ext)
        if not component.exists():
            logger.warning(
                f"Shapefile component '{component.name}' not found, skipping."
            )
            continue

        blob_name = f"{blob_prefix}/{component.name}"
        data = component.read_bytes()
        blob_storage_service.upload_file(
            container_name=StorageContainer.DATA,
            blob_name=blob_name,
            data=data,
        )

        logger.info(
            f"Uploaded '{component.name}' to '{blob_name}' in container '{StorageContainer.DATA.value}'"
        )
