import geopandas as gpd
from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection
from shapely import from_wkb
from sqlalchemy import Engine, text

from src import Config
from src.application.common import logger
from src.application.contracts import IFilePathService
from src.domain.enums import StorageContainer, Theme, EPSGCode
from src.infra.infrastructure import Containers


def setup_benchmarking_framework() -> None:
    _postgres_buildings_seed()


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

    with postgres_db_context.connect() as conn:
        row_count = conn.execute(text("SELECT COUNT(*) FROM buildings")).scalar_one()

        if row_count > 0:
            logger.info(f"Table 'buildings' already contains {row_count} rows. Aborting seed.")
            return

    logger.info(f"Inserting {building_gdf.shape[0]} rows into 'buildings' table...")
    with postgres_db_context.connect() as conn:
        building_gdf.to_postgis(
            name="buildings",
            con=conn,
            if_exists="replace",
            index=False
        )

    logger.info("Insertion completed")
