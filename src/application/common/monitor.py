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
from src.application.contracts import IMonitoringStorageService
from src.infra.infrastructure import Containers


def monitor_cpu_and_ram(query_id: str, interval: float = Config.DEFAULT_SAMPLE_TIMEOUT):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = None
            run_id = _get_run_id()

            for i in range(Config.BENCHMARK_RUNS):
                iteration = i + 1
                process = psutil.Process()
                samples: list[dict[str, Any]] = []

                _initialize_cpu_metrics(process=process)

                initial_timestamp, initial_user_cpu_time, initial_system_cpu_time = _initialize_timers(process=process)
                thread, thread_lock, thread_event = _initialize_threading(
                    target=_sampler,
                    process=process,
                    samples=samples,
                    initial_timestamp=initial_timestamp,
                    initial_user_cpu_time=initial_user_cpu_time,
                    initial_system_cpu_time=initial_system_cpu_time,
                    interval=interval
                )

                thread.start()
                try:
                    result = func(*args, **kwargs)
                finally:
                    thread_event.set()
                    thread.join(timeout=1.0)

                _save_run(run_id=run_id, query_id=query_id, iteration=iteration, samples=samples)

            _save_run_metadata(query_id=query_id, run_id=run_id)
            return result

        return wrapper

    return decorator


def _sampler(
        process: psutil.Process,
        thread_lock: threading.Lock,
        thread_event: threading.Event,
        samples: list[dict[str, Any]],
        previous_timestamp: float,
        previous_user_cpu_time: float,
        previous_system_cpu_time: float,
        interval: float
) -> None:
    cpu_count = _get_cpu_count()
    start_time = time.time()

    while not thread_event.wait(interval):
        try:
            wall_clock_timestamp, timestamp, user_cpu_time, system_cpu_time = _get_times(process)
            delta_cpu = (user_cpu_time + system_cpu_time) - (previous_user_cpu_time + previous_system_cpu_time)
            elapsed_time = timestamp - previous_timestamp

            cpu_percent = (delta_cpu / elapsed_time) * 100.0 / cpu_count if elapsed_time > 0 else 0.0
            rss = _get_rss(process)

            zeroed_wall_clock_time = wall_clock_timestamp - start_time

            with thread_lock:
                samples.append({
                    "wall_clock_time": zeroed_wall_clock_time,
                    "delta_time": elapsed_time,
                    "delta_cpu": delta_cpu,
                    "cpu_percentage": cpu_percent,
                    "rss": rss,
                })

            previous_timestamp = timestamp
            previous_user_cpu_time = user_cpu_time
            previous_system_cpu_time = system_cpu_time
        except Exception:
            pass


def _initialize_threading(
        target: object | None,
        process: psutil.Process,
        samples: list[dict[str, Any]],
        initial_timestamp: float,
        initial_user_cpu_time: float,
        initial_system_cpu_time: float,
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
        "previous_timestamp": initial_timestamp,
        "previous_user_cpu_time": initial_user_cpu_time,
        "previous_system_cpu_time": initial_system_cpu_time,
        "interval": interval
    }

    thread = threading.Thread(target=target, daemon=True, kwargs=kwargs)

    return thread, thread_lock, thread_event


def _initialize_cpu_metrics(process: psutil.Process) -> None:
    # The initial `cpu_percent` returns a meaningless value. See 'https://psutil.readthedocs.io/en/latest/#psutil.cpu_percent' for more information
    process.cpu_percent(interval=None)
    time.sleep(0.1)
    process.cpu_percent(interval=None)


def _initialize_timers(process: psutil.Process) -> tuple[float, float, float]:
    _, initial_timestamp, initial_user_cpu_time, initial_system_cpu_time = _get_times(process)
    return initial_timestamp, initial_user_cpu_time, initial_system_cpu_time


def _get_times(process: psutil.Process) -> tuple[float, float, float, float]:
    cpu_times = process.cpu_times()

    user_cpu_time = cpu_times.user
    system_cpu_time = cpu_times.system
    timestamp = time.perf_counter()
    wall_clock_timestamp = time.time()

    return wall_clock_timestamp, timestamp, user_cpu_time, system_cpu_time


def _get_cpu_count() -> int:
    return psutil.cpu_count(logical=True) or 1


def _get_rss(process: psutil.Process) -> float:
    rss = process.memory_info().rss
    return rss


@inject
def _get_run_id(run_id: str = Provide[Containers.config.run_id]) -> str:
    if run_id is not None:
        return run_id

    today = date.today().strftime("%Y-%m-%d")
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

    return f"{today}-{suffix}"


def _save_run(
        run_id: str,
        query_id: str,
        iteration: int,
        samples: list[dict[str, Any]],
        monitoring_storage_service: IMonitoringStorageService = Provide[Containers.monitoring_storage_service],
) -> None:
    monitoring_storage_service.write_run_to_blob_storage(
        samples=samples,
        query_id=query_id,
        run_id=run_id,
        iteration=iteration
    )


def _save_run_metadata(
        query_id: str,
        run_id: str,
        monitoring_storage_service: IMonitoringStorageService = Provide[Containers.monitoring_storage_service]
) -> None:
    metadata_id = str(uuid.uuid4())
    timestamp = datetime.datetime.now(timezone.utc)
    monitoring_storage_service.write_metadata_to_blob_storage(
        metadata_id=metadata_id,
        timestamp=timestamp,
        query_id=query_id,
        run_id=run_id
    )
