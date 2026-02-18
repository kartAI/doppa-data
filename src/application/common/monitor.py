import functools
import threading
import time
from builtins import function
from typing import Any

import pandas as pd
import psutil
from dependency_injector.wiring import Provide

from src import Config
from src.application.contracts import IBlobStorageService, IBytesService
from src.domain.enums import StorageContainer
from src.infra.infrastructure import Containers


def monitor_cpu_and_ram(run_id: str, query_id: str, interval: float = 0.05):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, 5):
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

                _save_run(run_id=run_id, query_id=query_id, execution_number=i, samples=samples)

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

    while not thread_event.wait(interval):
        try:
            wall_clock_timestamp, timestamp, user_cpu_time, system_cpu_time = _get_times(process)
            delta_cpu = (user_cpu_time + system_cpu_time) - (previous_user_cpu_time + previous_system_cpu_time)
            elapsed_time = timestamp - previous_timestamp

            cpu_percent = (delta_cpu / elapsed_time) * 100.0 / cpu_count if elapsed_time > 0 else 0.0
            rss = _get_rss(process)

            with thread_lock:
                samples.append({
                    "wall_clock_time": wall_clock_timestamp,
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
        target: function,
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


def _initialize_ram_metrics(process: psutil.Process) -> float:
    return _get_rss(process)


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


def _save_run(
        run_id: str,
        query_id: str,
        execution_number: int,
        samples: list[dict[str, Any]],
        blob_storage_service: IBlobStorageService = Provide[Containers.blob_storage_service],
        bytes_service: IBytesService = Provide[Containers.bytes_service]
) -> None:
    parquet_file_name = f"{query_id}-{run_id}-{execution_number}.parquet"
    df = pd.DataFrame(samples)
    df_bytes = bytes_service.convert_df_to_parquet_bytes(df=df)
    blob_storage_service.upload_file(
        container_name=StorageContainer.BENCHMARKS,
        blob_name=parquet_file_name,
        data=df_bytes
    )
