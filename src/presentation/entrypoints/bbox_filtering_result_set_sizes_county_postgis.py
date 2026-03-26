from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine, text

from src.application.common.monitor_network import monitor_network
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration, BoundingBox
from src.infra.infrastructure import Containers


def bbox_filtering_result_set_sizes_county_postgis() -> None:
    _benchmark()


@inject
@monitor_network(
    query_id="bbox-filtering-result-set-sizes-county-postgis",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_RESULT_SET_SIZES,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def _benchmark(
        db_context: Engine = Provide[Containers.postgres_context],
) -> None:
    min_lon, min_lat, max_lon, max_lat = BoundingBox.TRONDELAG_WGS84.value

    sql = text(
        """
        SELECT * FROM buildings
        WHERE ST_Intersects(
            geometry,
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
        );
        """
    )

    with db_context.connect() as conn:
        conn.execute(
            sql,
            {
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat,
            },
        ).fetchall()
