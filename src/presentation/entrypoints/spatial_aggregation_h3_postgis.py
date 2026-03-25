from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine, text

from src.application.common.monitor_network import monitor_network
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor_network(
    query_id="spatial-aggregation-h3-postgis",
    benchmark_iteration=BenchmarkIteration.SPATIAL_AGGREGATION_H3,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def spatial_aggregation_h3_postgis(
        db_context: Engine = Provide[Containers.postgres_context],
) -> None:
    sql = text(
        """
        WITH building_centroids AS (
            SELECT ST_Centroid(geometry) AS centroid
            FROM buildings
            WHERE ST_IsValid(geometry)
        )
        SELECT h3_latlng_to_cell(
                       ST_Y(centroid),
                       ST_X(centroid),
                       7
               )        AS h3_cell,
               COUNT(*) AS building_count
        FROM building_centroids
        GROUP BY h3_cell
        ORDER BY building_count DESC;
        """
    )

    with db_context.connect() as conn:
        _ = conn.execute(sql).fetchall()
