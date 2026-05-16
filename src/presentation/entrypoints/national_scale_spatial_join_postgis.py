import geopandas as gpd
from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection
from shapely import from_wkb
from sqlalchemy import Engine, text

from src import Config
from src.application.common import logger
from src.application.common.monitor import monitor
from src.application.dtos import CostConfiguration
from src.domain.enums import StorageContainer, BenchmarkIteration, EPSGCode
from src.infra.infrastructure import Containers


@inject
def national_scale_spatial_join_postgis(
    duckdb_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
    postgres_context: Engine = Provide[Containers.postgres_context],
) -> None:
    """
    Benchmark: national-scale spatial join between Norwegian counties and the small
    buildings dataset using PostGIS. Seeds the ``counties`` table from blob storage
    via DuckDB before running the timed per-county building count aggregation.
    """
    _seed_counties(duckdb_context=duckdb_context, postgres_context=postgres_context)
    _benchmark()


def _seed_counties(
    duckdb_context: DuckDBPyConnection, postgres_context: Engine
) -> None:
    counties_path = f"az://{StorageContainer.METADATA.value}/{Config.DATABRICKS_MUNICIPALITIES_FILE}"

    logger.info(f"Loading counties from '{counties_path}' into PostgreSQL...")
    df = duckdb_context.execute(f"""
        SELECT region AS county_name, wkb AS geometry
        FROM read_parquet('{counties_path}')
    """).fetchdf()

    df["geometry"] = df["geometry"].apply(
        lambda g: bytes(g) if isinstance(g, (memoryview, bytearray)) else g
    )
    df["geometry"] = df["geometry"].apply(from_wkb)

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=EPSGCode.WGS84.value)

    with postgres_context.connect() as conn:
        gdf.to_postgis("counties", con=conn, if_exists="replace", index=False)

    logger.info(f"Seeded {len(gdf)} counties into PostgreSQL.")


@inject
@monitor(
    query_id="national-scale-spatial-join-postgis",
    benchmark_iteration=BenchmarkIteration.NATIONAL_SCALE_SPATIAL_JOIN,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True),
)
def _benchmark(
    db_context: Engine = Provide[Containers.postgres_context],
) -> list:
    sql = text("""
        SELECT
            c.county_name,
            COUNT(*) AS building_count
        FROM counties c
        JOIN buildings_small b ON ST_Intersects(c.geometry, b.geometry)
        GROUP BY c.county_name
        ORDER BY building_count DESC
    """)

    with db_context.connect() as conn:
        return conn.execute(sql).fetchall()
