import datetime
import functools
import time
from typing import Any

import psutil

from src.application.common import logger
from src.application.common.monitor_utils import (
    _get_run_id,
    _get_benchmark_run,
    _save_run,
    _save_run_metadata,
    _save_run_cost_analytics,
)
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration, BlobOperationType


def monitor_databricks(
    query_id: str,
    benchmark_iteration: BenchmarkIteration,
    cost_configuration: CostConfiguration,
):
    """
    Monitoring decorator for Databricks jobs. Identical to monitor but without warmup
    iterations, since each Databricks run provisions a cluster (warmup would multiply cost).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = None

            run_id = _get_run_id()
            benchmark_run = _get_benchmark_run()

            logger.info(
                f"Starting Databricks benchmark for query '{query_id}' with run ID '{run_id}'."
            )
            logger.info(
                f"Executing {benchmark_iteration.value} benchmark run(s) (no warmup)."
            )

            ingress_sum: int = 0
            egress_sum: int = 0
            start_time = datetime.datetime.now(datetime.UTC)
            total = benchmark_iteration.value

            for i in range(total):
                iteration = i + 1
                logger.info(f"Starting iteration {iteration}/{total}...")
                result, wall_elapsed_time, net_bytes_sent, net_bytes_received = (
                    _timed_call(func, *args, **kwargs)
                )
                # Use API-reported notebook execution time (excludes cluster provisioning/teardown),
                # not wall_elapsed_time which includes provisioning overhead.
                elapsed_time: float = result
                ingress_sum += net_bytes_received
                egress_sum += net_bytes_sent

                logger.info(
                    f"Iteration {iteration}/{total} completed in {round(elapsed_time, 2)}s "
                    f"(execution only, excludes provisioning)."
                )

                _save_run(
                    run_id=run_id,
                    benchmark_run=benchmark_run,
                    query_id=query_id,
                    iteration=iteration,
                    total_iterations=total,
                    samples=[
                        {
                            "elapsed_time": elapsed_time,
                            "network_bytes_sent": net_bytes_sent,
                            "network_bytes_received": net_bytes_received,
                        }
                    ],
                )

            end_time = datetime.datetime.now(datetime.UTC)
            total_seconds = round((end_time - start_time).total_seconds(), 2)
            logger.info(
                f"All {total} iteration(s) completed in {total_seconds}s. Saving results..."
            )

            _save_run_metadata(query_id=query_id, run_id=run_id)
            _save_run_cost_analytics(
                run_id=run_id,
                cost_configuration=cost_configuration,
                query_id=query_id,
                start_time=start_time,
                end_time=end_time,
                bytes_ingress=ingress_sum,
                bytes_egress=egress_sum,
                operation_type=BlobOperationType.READ,
            )

            logger.info(f"Benchmark run {benchmark_run} complete.")
            return result

        return wrapper

    return decorator


def _timed_call(func, *args, **kwargs) -> tuple[Any, float, int, int]:
    before = psutil.net_io_counters()
    start_time = time.perf_counter()

    result = func(*args, **kwargs)

    end_time = time.perf_counter()
    after = psutil.net_io_counters()

    elapsed_time = end_time - start_time
    network_bytes_sent = after.bytes_sent - before.bytes_sent
    network_bytes_received = after.bytes_recv - before.bytes_recv

    return result, elapsed_time, network_bytes_sent, network_bytes_received
