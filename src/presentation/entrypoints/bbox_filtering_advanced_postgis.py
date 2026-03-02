from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine

from src.application.common.monitor import monitor
from src.infra.infrastructure import Containers


@inject
@monitor(query_id="bbox-filtering-advanced-postgis")
def bbox_filtering_advanced_postgis(
        db_context: Engine = Provide[Containers.postgres_context]
) -> None:
    pass
