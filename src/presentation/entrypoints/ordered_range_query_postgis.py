from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine, text

from src.application.common.monitor import monitor
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration, BoundingBox
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="ordered-range-query-postgis",
    benchmark_iteration=BenchmarkIteration.ORDERED_RANGE_QUERY,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def ordered_range_query_postgis(
        db_context: Engine = Provide[Containers.postgres_context],
) -> list:
    min_lon, min_lat, max_lon, max_lat = BoundingBox.TRONDELAG_WGS84.value

    sql = text(
        """
        SELECT *
        FROM buildings
        WHERE ST_Intersects(
                      geometry,
                      ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              )
        ORDER BY building_id
        LIMIT 1000;
        """
    )

    with db_context.connect() as conn:
        return conn.execute(sql, {
            "min_lon": min_lon, "min_lat": min_lat,
            "max_lon": max_lon, "max_lat": max_lat,
        }).fetchall()
