from dependency_injector.wiring import inject, Provide
from sqlalchemy import Engine, text

from src.application.common.monitor_network import monitor_network
from src.domain.enums import BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor_network(query_id="db-scan-postgis", benchmark_iteration=BenchmarkIteration.DB_SCAN)
def db_scan_postgis(
        db_context: Engine = Provide[Containers.postgres_context]
) -> None:
    with db_context.connect() as conn:
        conn.execute(text("SELECT count(*) AS count FROM buildings")).scalar_one()
