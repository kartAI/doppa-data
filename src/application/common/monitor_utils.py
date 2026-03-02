import datetime
import random
import string
import uuid
from datetime import date
from typing import Any

from dependency_injector.wiring import inject, Provide

from src import Config
from src.application.common import logger
from src.application.contracts import IMonitoringStorageService
from src.infra.infrastructure import Containers


@inject
def _get_run_id(run_id: str = Provide[Containers.config.run_id]) -> str:
    if run_id is not None:
        logger.debug(f"Found run ID '{run_id}' from DI.")
        return run_id

    today = date.today().strftime("%Y-%m-%d")
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=Config.RUN_ID_LENGTH))
    run_id = f"{today}-{suffix}"
    logger.info(f"No run ID from DI. Created run ID '{run_id}'")

    return run_id


@inject
def _get_benchmark_run(benchmark_run: int = Provide[Containers.config.benchmark_run]) -> int:
    return benchmark_run


@inject
def _save_run(
        run_id: str,
        benchmark_run: int,
        query_id: str,
        iteration: int,
        samples: list[dict[str, Any]],
        monitoring_storage_service: IMonitoringStorageService = Provide[Containers.monitoring_storage_service],
) -> None:
    iteration = _create_global_iteration(iteration=iteration, benchmark_run=benchmark_run)
    monitoring_storage_service.write_run_to_blob_storage(
        samples=samples,
        query_id=query_id,
        run_id=run_id,
        benchmark_run=benchmark_run,
        iteration=iteration
    )


@inject
def _save_run_metadata(
        query_id: str,
        run_id: str,
        monitoring_storage_service: IMonitoringStorageService = Provide[Containers.monitoring_storage_service]
) -> None:
    logger.info("Saving benchmark metadata to blob storage.")
    metadata_id = str(uuid.uuid4())
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    monitoring_storage_service.write_metadata_to_blob_storage(
        metadata_id=metadata_id,
        timestamp=timestamp,
        query_id=query_id,
        run_id=run_id
    )

    logger.info(f"Benchmark metadata saved with ID '{metadata_id}'.")


def _create_global_iteration(iteration: int, benchmark_run: int) -> int:
    return iteration + Config.BENCHMARK_ITERATIONS * (benchmark_run - 1)
