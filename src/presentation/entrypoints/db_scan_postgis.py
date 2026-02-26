from dependency_injector.wiring import inject, Provide
from sqlalchemy import Engine, text

from src.application.common.monitor import monitor_cpu_and_ram
from src.infra.infrastructure import Containers


@inject
@monitor_cpu_and_ram(query_id="db-scan-postgis")
def db_scan_postgis(
        db_context: Engine = Provide[Containers.postgres_context]
) -> None:
    with db_context.connect() as conn:
        conn.execute(text("SELECT count(*) AS count FROM buildings")).scalar_one()
