from dependency_injector.wiring import inject, Provide
from psycopg2.extensions import cursor

from src.application.common.monitor import monitor_cpu_and_ram
from src.infra.infrastructure import Containers


@inject
@monitor_cpu_and_ram(query_id="db-scan-postgis")
def db_scan_postgis(
        db_context: cursor = Provide[Containers.postgres_context]
) -> None:
    db_context.execute("SELECT count(*) AS count FROM 'buildings'")
