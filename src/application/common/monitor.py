import datetime
import functools
import random
import string
import threading
import time
import uuid
from datetime import date, timezone
from typing import Any

import psutil
from dependency_injector.wiring import Provide, inject

from src import Config
from src.application.common import logger
from src.application.contracts import IMonitoringStorageService
from src.infra.infrastructure import Containers


def monitor(query_id: str, interval: float = Config.DEFAULT_SAMPLE_TIMEOUT):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = None

            run_id = _get_run_id()
            benchmark_run = _get_benchmark_run()

            process = psutil.Process()

            logger.info(f"Starting benchmark for query '{query_id}' with run ID '{run_id}'.")
            logger.info(f"Executing {Config.BENCHMARK_WARMUP_ITERATIONS} warmup runs.")
            for _ in range(Config.BENCHMARK_WARMUP_ITERATIONS):
                func(*args, **kwargs)

            logger.info(f"Warmup runs completed. Starting {Config.BENCHMARK_ITERATIONS} benchmark runs.")
            logger.info(f"Benchmarking started with sampling interval of {interval} seconds.")
            for i in range(Config.BENCHMARK_ITERATIONS):
                iteration = i + 1
                samples: list[dict[str, Any]] = []

                _initialize_cpu_metrics(process=process)

                thread, thread_lock, thread_event = _initialize_threading(
                    target=_sampler,
                    process=process,
                    samples=samples,
                    interval=interval
                )

                thread.start()
                try:
                    result = func(*args, **kwargs)
                finally:
                    thread_event.set()
                    thread.join(timeout=1.0)

                _save_run(
                    run_id=run_id,
                    query_id=query_id,
                    benchmark_run=benchmark_run,
                    iteration=iteration,
                    samples=samples
                )

            logger.info(f"Benchmarking completed.")
            _save_run_metadata(query_id=query_id, run_id=run_id)
            return result

        return wrapper

    return decorator


def _sampler(
        process: psutil.Process,
        thread_lock: threading.Lock,
        thread_event: threading.Event,
        samples: list[dict[str, Any]],
        interval: float
) -> None:
    start_timestamp, start_process_cpu_time, _, start_system_cpu_time_per_core = _get_cpu_metrics(process)
    previous_timestamp = start_timestamp
    previous_process_cpu_time = start_process_cpu_time

    while not thread_event.wait(interval):
        try:
            timestamp, process_cpu_time, cpu_percent, system_cpu_time_per_core = _get_cpu_metrics(process)
            rss = _get_rss(process)

            delta_process_cpu_time = process_cpu_time - previous_process_cpu_time
            elapsed_time = timestamp - previous_timestamp

            zeroed_elapsed_time = timestamp - start_timestamp
            core_structs = {
                f"core_{core_id}": {
                    "user": t["user"] - start_system_cpu_time_per_core[core_id]["user"],
                    "system": t["system"] - start_system_cpu_time_per_core[core_id]["system"],
                    "idle": t["idle"] - start_system_cpu_time_per_core[core_id]["idle"],
                    "iowait": t["iowait"] - start_system_cpu_time_per_core[core_id]["iowait"],
                    "percent": t["percent"]
                }
                for core_id, t in system_cpu_time_per_core.items()
            }

            with thread_lock:

                samples.append({
                    "elapsed_time": zeroed_elapsed_time,
                    "delta_time": elapsed_time,
                    "delta_process_cpu_time": delta_process_cpu_time,
                    "process_cpu_percentage": cpu_percent,
                    "rss": rss,
                    **core_structs
                })

            previous_timestamp = timestamp
            previous_process_cpu_time = process_cpu_time
        except Exception as e:
            logger.error(f"Sampling error in _sampler: {e}")


def _initialize_threading(
        target: object | None,
        process: psutil.Process,
        samples: list[dict[str, Any]],
        interval: float
) -> tuple[
    threading.Thread,
    threading.Lock,
    threading.Event
]:
    thread_lock = threading.Lock()
    thread_event = threading.Event()

    kwargs = {
        "process": process,
        "thread_lock": thread_lock,
        "thread_event": thread_event,
        "samples": samples,
        "interval": interval
    }

    thread = threading.Thread(target=target, daemon=True, kwargs=kwargs)

    return thread, thread_lock, thread_event


def _initialize_cpu_metrics(process: psutil.Process) -> None:
    # The initial `cpu_percent` returns a meaningless value. See 'https://psutil.readthedocs.io/en/latest/#psutil.cpu_percent' for more information
    process.cpu_percent(interval=None)
    psutil.cpu_percent(percpu=True)
    time.sleep(0.1)
    process.cpu_percent(interval=None)
    psutil.cpu_percent(percpu=True)


def _get_cpu_metrics(process: psutil.Process) -> tuple[float, float, float, dict[int, dict[str, float]]]:
    cpu_times = process.cpu_times()
    cpu_percent = process.cpu_percent()

    per_core_raw = psutil.cpu_times(percpu=True)
    per_core_percent = psutil.cpu_percent(percpu=True)

    system_metrics_per_core = {
        i + 1: {
            "user": c.user,
            "system": c.system,
            "idle": c.idle,
            "iowait": getattr(c, "iowait", 0.0),
            "percent": per_core_percent[i]
        }
        for i, c in enumerate(per_core_raw)
    }

    user_cpu_time = cpu_times.user
    system_cpu_time = cpu_times.system
    process_cpu_time = user_cpu_time + system_cpu_time

    timestamp = time.perf_counter()

    return timestamp, process_cpu_time, cpu_percent, system_metrics_per_core


def _get_cpu_count() -> int:
    return psutil.cpu_count(logical=True) or 1


def _get_rss(process: psutil.Process) -> float:
    rss = process.memory_info().rss
    return rss


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


def _save_run_metadata(
        query_id: str,
        run_id: str,
        monitoring_storage_service: IMonitoringStorageService = Provide[Containers.monitoring_storage_service]
) -> None:
    logger.info("Saving benchmark metadata to blob storage.")
    metadata_id = str(uuid.uuid4())
    timestamp = datetime.datetime.now(timezone.utc)
    monitoring_storage_service.write_metadata_to_blob_storage(
        metadata_id=metadata_id,
        timestamp=timestamp,
        query_id=query_id,
        run_id=run_id
    )

    logger.info(f"Benchmark metadata saved with ID '{metadata_id}'.")


def _create_global_iteration(iteration: int, benchmark_run: int) -> int:
    return iteration + Config.BENCHMARK_ITERATIONS * (benchmark_run - 1)
