from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine, text

from src.application.common.monitor import monitor
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="spatial-aggregation-grid-postgis",
    benchmark_iteration=BenchmarkIteration.SPATIAL_AGGREGATION_GRID,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def spatial_aggregation_grid_postgis(
        db_context: Engine = Provide[Containers.postgres_context],
) -> list:
    """
    Benchmark: spatial aggregation on the seeded ``buildings_small`` table using
    PostGIS. Bins each building centroid into a 0.01 degree lat/lon grid cell and
    returns per-cell counts ordered by count.
    """
    sql = text(
        """
        WITH building_centroids AS (
            SELECT ST_Centroid(geometry) AS centroid
            FROM buildings_small
            WHERE ST_IsValid(geometry)
        )
        SELECT
            FLOOR(ST_Y(centroid) / 0.01) AS lat_cell,
            FLOOR(ST_X(centroid) / 0.01) AS lng_cell,
            COUNT(*) AS building_count
        FROM building_centroids
        GROUP BY lat_cell, lng_cell
        ORDER BY building_count DESC;
        """
    )

    with db_context.connect() as conn:
        return conn.execute(sql).fetchall()
