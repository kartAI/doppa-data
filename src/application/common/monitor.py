import datetime
import functools

from src import Config
from src.application.common import logger
from src.application.common.monitor_utils import (
    _get_run_id,
    _get_benchmark_run,
    _measure_net_io,
    _save_run,
    _save_run_metadata,
    _save_run_cost_analytics,
)
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration, BlobOperationType


def monitor(
    query_id: str,
    benchmark_iteration: BenchmarkIteration,
    cost_configuration: CostConfiguration,
    skip_warmup: bool = False,
    elapsed_from_result: bool = False,
):
    """
    Benchmarking decorator. Wraps a function in warmup + timed iterations, records
    per-iteration samples, and writes run metadata and cost analytics to blob storage.
    :param query_id: Identifier for the benchmarked query.
    :param benchmark_iteration: Number of timed iterations to run.
    :param cost_configuration: Which Azure cost components to compute and store.
    :param skip_warmup: Disable warmup runs. Use for Databricks, since each run provisions a cluster and warmup would multiply cost. Default is False.
    :param elapsed_from_result: Treat the wrapped function's return value as the elapsed time in seconds instead of wall-clock. Use for Databricks, since the API-reported notebook execution time excludes cluster provisioning/teardown. Default is False.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = None

            run_id = _get_run_id()
            benchmark_run = _get_benchmark_run()

            logger.info(
                f"Starting benchmark for query '{query_id}' with run ID '{run_id}'."
            )

            if skip_warmup:
                logger.info(
                    f"Executing {benchmark_iteration.value} benchmark run(s) (no warmup)."
                )
            else:
                logger.info(
                    f"Executing {Config.BENCHMARK_WARMUP_ITERATIONS} warmup runs."
                )
                for _ in range(Config.BENCHMARK_WARMUP_ITERATIONS):
                    func(*args, **kwargs)
                logger.info(
                    f"Warmup runs completed. Starting {benchmark_iteration.value} benchmark runs."
                )

            ingress_sum: int = 0
            egress_sum: int = 0
            start_time = datetime.datetime.now(datetime.UTC)

            for i in range(benchmark_iteration.value):
                iteration = i + 1
                result, wall_elapsed_time, net_bytes_sent, net_bytes_received = (
                    _measure_net_io(func, *args, **kwargs)
                )
                elapsed_time = result if elapsed_from_result else wall_elapsed_time

                ingress_sum += net_bytes_received
                egress_sum += net_bytes_sent

                _save_run(
                    run_id=run_id,
                    benchmark_run=benchmark_run,
                    query_id=query_id,
                    iteration=iteration,
                    total_iterations=benchmark_iteration.value,
                    samples=[
                        {
                            "elapsed_time": elapsed_time,
                            "network_bytes_sent": net_bytes_sent,
                            "network_bytes_received": net_bytes_received,
                        }
                    ],
                )

            end_time = datetime.datetime.now(datetime.UTC)
            logger.info(
                f"Benchmark runs completed in {round((end_time - start_time).total_seconds(), 2)} seconds."
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

            logger.info(f"Benchmark run {benchmark_run} completed.")
            return result

        return wrapper

    return decorator
