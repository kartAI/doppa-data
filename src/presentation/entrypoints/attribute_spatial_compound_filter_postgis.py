from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine, text

from src.application.common.monitor_network import monitor_network
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration, BoundingBox, DataSource
from src.infra.infrastructure import Containers


@inject
@monitor_network(
    query_id="attribute-spatial-compound-filter-postgis",
    benchmark_iteration=BenchmarkIteration.ATTRIBUTE_SPATIAL_COMPOUND_FILTER,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def attribute_spatial_compound_filter_postgis(
        db_context: Engine = Provide[Containers.postgres_context],
) -> None:
    min_lon, min_lat, max_lon, max_lat = BoundingBox.NEIGHBORHOOD_WGS84.value

    sql = text(
        """
        SELECT *
        FROM buildings
        WHERE source = :source
          AND ST_Intersects(
                geometry,
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              );
        """
    )

    with db_context.connect() as conn:
        result = conn.execute(sql, {
            "source": DataSource.OSM.value,
            "min_lon": min_lon, "min_lat": min_lat,
            "max_lon": max_lon, "max_lat": max_lat,
        })

        result.fetchall()
