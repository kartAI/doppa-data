import json
import os
import random
import shutil
import string
import subprocess
import time
from datetime import date
from typing import Any

import yaml

from src import Config
from src.application.common import logger
from src.domain.enums import StorageContainer


def main() -> None:
    with open(Config.BENCHMARK_FILE) as f:
        benchmark_configuration = yaml.safe_load(f)

    run_id = _create_run_id()
    logger.info(f"Starting benchmark run '{run_id}'")

    for benchmark_run in range(1, Config.BENCHMARK_RUNS + 1):
        _run_benchmarks(run_id=run_id, benchmark_run=benchmark_run, benchmark_configuration=benchmark_configuration)


def _run_benchmarks(run_id: str, benchmark_run: int, benchmark_configuration: Any) -> None:
    for experiment in benchmark_configuration["experiments"]:
        experiment_id = experiment["id"]
        container_group_name = f"benchmark-{experiment_id}"

        docker_image = experiment["image"]

        cpu = str(experiment["cpu"])
        memory_gb = str(experiment["memory_gb"])

        logger.info(
            f"Run {benchmark_run}/{Config.BENCHMARK_RUNS} - Experiment '{experiment_id}' | cpu={cpu} | memory={memory_gb} | run_id='{run_id}'"
        )

        _delete_container_instance(container_group_name=container_group_name)
        _create_container_instance(
            run_id=run_id,
            benchmark_run=benchmark_run,
            experiment_id=experiment_id,
            container_group_name=container_group_name,
            docker_image=docker_image,
            cpu=cpu,
            memory_gb=memory_gb
        )
        _check_container_state(container_group_name=container_group_name)
        _delete_container_instance(container_group_name=container_group_name)

    logger.info(f"Completed benchmark run '{run_id}' - iteration {benchmark_run}")


def _create_run_id() -> str:
    date_prefix = date.today().isoformat()
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=Config.RUN_ID_LENGTH))

    return f"{date_prefix}-{suffix}"


# noinspection PyDeprecation
def _run_cmd(cmd: list[str], suppress_error_log: bool = False) -> str:
    az_path = shutil.which(cmd[0])
    if az_path is not None:
        cmd[0] = az_path

    result = subprocess.run(cmd, capture_output=True, text=True, check=False, shell=False)

    if result.returncode != 0:
        cmd_str = " ".join(cmd)
        if not suppress_error_log:
            stderr = result.stderr.strip()
            stdout = result.stdout.strip()
            logger.error(
                "Command failed (exit %s): %s | stderr: %s%s",
                result.returncode,
                cmd_str,
                stderr,
                f" | stdout: {stdout}" if stdout else ""
            )
        else:
            logger.debug("Soft-check command failure: %s | stderr: %s", cmd_str, result.stderr.strip())

        raise RuntimeError(f"Command failed with exit code {result.returncode}")

    return result.stdout


def _container_exists(container_group_name: str) -> bool:
    check_cmd = [
        "az", "container", "show",
        "--resource-group", Config.AZURE_RESOURCE_GROUP,
        "--name", container_group_name
    ]

    try:
        _run_cmd(check_cmd, suppress_error_log=True)
        return True
    except RuntimeError:
        return False


def _delete_container_instance(container_group_name: str) -> None:
    if not _container_exists(container_group_name):
        logger.info(f"Container group '{container_group_name}' does not exist. Skipping deletion.")
        return

    delete_command = [
        "az", "container", "delete",
        "--resource-group", Config.AZURE_RESOURCE_GROUP,
        "--name", container_group_name,
        "--yes"
    ]

    logger.info(f"Deleting container group '{container_group_name}'...")
    _run_cmd(delete_command)
    logger.info(f"Deleted container group '{container_group_name}'")


def _create_container_instance(
        run_id: str,
        benchmark_run: int,
        experiment_id: str,
        container_group_name: str,
        docker_image: str,
        cpu: str,
        memory_gb: str,
) -> None:
    acr_login_server = os.getenv("ACR_LOGIN_SERVER")
    acr_username = os.getenv("ACR_USERNAME")
    acr_password = os.getenv("ACR_PASSWORD")

    startup_command = (
        f"python benchmark_runner.py "
        f"--script-id {experiment_id} "
        f"--benchmark-run {benchmark_run} "
        f"--run-id {run_id}"
    )

    create_command = [
        "az", "container", "create",
        "--resource-group", Config.AZURE_RESOURCE_GROUP,
        "--name", container_group_name,
        "--image", docker_image,
        "--location", Config.AZURE_RESOURCE_LOCATION,
        "--restart-policy", "Never",

        "--os-type", "Linux",
        "--cpu", cpu,
        "--memory", memory_gb,

        "--command-line", startup_command,

        "--registry-login-server", acr_login_server,
        "--registry-username", acr_username,
        "--registry-password", acr_password,

        "--environment-variables",
        f"AZURE_BLOB_STORAGE_BENCHMARK_CONTAINER={StorageContainer.BENCHMARKS.value}",
        f"AZURE_BLOB_STORAGE_METADATA_CONTAINER={StorageContainer.METADATA.value}",

        "--secure-environment-variables",
        f"AZURE_BLOB_STORAGE_CONNECTION_STRING={Config.AZURE_BLOB_STORAGE_CONNECTION_STRING}",
        f"POSTGRES_USERNAME={Config.POSTGRES_USERNAME}",
        f"POSTGRES_PASSWORD={Config.POSTGRES_PASSWORD}",

        "--no-wait"
    ]

    logger.info(f"Creating container group '{container_group_name}'...")
    _run_cmd(create_command)
    logger.info(
        "Created container group '%s' (experiment=%s, CPU=%s cores, RAM=%s GB) - startup: %s",
        container_group_name,
        experiment_id,
        cpu,
        memory_gb,
        startup_command
    )


def _check_container_state(container_group_name: str, poll_interval_seconds: float = 20) -> None:
    while True:
        show_command = [
            "az", "container", "show",
            "--resource-group", Config.AZURE_RESOURCE_GROUP,
            "--name", container_group_name,
            "--output", "json"
        ]

        data = json.loads(_run_cmd(show_command))
        state = data["instanceView"]["state"]

        match state:
            case "Succeeded":
                logger.info(f"Container '{container_group_name}' | State: '{state}' | Benchmark run completed.")
                break
            case "Failed":
                logger.error(
                    f"Container '{container_group_name}' failed. Please check the logs for more information."
                )
                raise RuntimeError(
                    f"Container '{container_group_name}' failed. Please check the logs for more information."
                )
            case _:
                logger.info(
                    f"Container '{container_group_name}' | State: '{state}' | Checking again in {poll_interval_seconds} seconds..."
                )
                time.sleep(poll_interval_seconds)


if __name__ == '__main__':
    main()
