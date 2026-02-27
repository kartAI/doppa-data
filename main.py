import json
import os
import random
import shutil
import string
import subprocess
import time
from datetime import date

import yaml

from src import Config
from src.application.common import logger
from src.domain.enums import StorageContainer


def _create_run_id() -> str:
    date_prefix = date.today().isoformat()
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=Config.RUN_ID_LENGTH))

    return f"{date_prefix}-{suffix}"


def _run_cmd(cmd: list[str]) -> str:
    az_path = shutil.which(cmd[0])
    if az_path is not None:
        cmd[0] = az_path

    result = subprocess.run(cmd, capture_output=True, text=True, check=False, shell=False)

    if result.returncode != 0:
        cmd_str = " ".join(cmd)
        logger.info(f"Command: {cmd_str}")
        logger.error(result.stderr)
        raise RuntimeError(f"Command failed with exit code {result.returncode}")

    return result.stdout


def _delete_container_instance(container_group_name: str) -> None:
    delete_command = [
        "az", "container", "delete",
        "--resource-group", Config.AZURE_RESOURCE_GROUP,
        "--name", container_group_name,
        "--yes"
    ]

    logger.info(f"Deleting container group '{container_group_name}...'")
    _run_cmd(delete_command)
    logger.info(f"Deleted container group '{container_group_name}'")


def _create_container_instance(
        run_id: str,
        experiment_id: str,
        container_group_name: str,
        docker_image: str,
        cpu: str,
        memory_gb: str,
) -> None:
    acr_login_server = os.getenv("ACR_LOGIN_SERVER")
    acr_username = os.getenv("ACR_USERNAME")
    acr_password = os.getenv("ACR_PASSWORD")

    startup_command = f"python benchmark_runner.py --script-id {experiment_id} --run-id {run_id}"

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
        "--environment-variables", f"AZURE_BLOB_STORAGE_BENCHMARK_CONTAINER={StorageContainer.BENCHMARKS.value}",
        "--secure-environment-variables",
        f"AZURE_BLOB_STORAGE_CONNECTION_STRING={Config.AZURE_BLOB_STORAGE_CONNECTION_STRING}",
        f"POSTGRES_USERNAME={Config.POSTGRES_USERNAME}",
        f"POSTGRES_PASSWORD={Config.POSTGRES_PASSWORD}",
        "--no-wait"
    ]

    logger.info(f"Creating container group '{container_group_name}'...")
    _run_cmd(create_command)
    logger.info(
        f"Created container group '{container_group_name}' (CPU: {cpu} cores | RAM: {memory_gb} GB) with startup command '{startup_command}'"
    )


def _check_container_state(container_group_name: str, timeout: float = 5) -> None:
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
                logger.info(f"Container '{container_group_name}' has stopped with state '{state}'")
                break
            case "Failed":
                raise RuntimeError(
                    f"Container '{container_group_name}' failed. Please check the logs for more information."
                )
            case _:
                logger.info(
                    f"Current container state for '{container_group_name}' is '{state}'. Checking again in {timeout} seconds..."
                )

                time.sleep(timeout)


def main() -> None:
    with open(Config.BENCHMARK_FILE) as f:
        benchmark_configuration = yaml.safe_load(f)

    run_id = _create_run_id()

    for experiment in benchmark_configuration["experiments"]:
        experiment_id = experiment["id"]
        container_group_name = f"benchmark-{experiment_id}"

        docker_image = experiment["image"]

        cpu = str(experiment["cpu"])
        memory_gb = str(experiment["memory_gb"])

        _delete_container_instance(container_group_name=container_group_name)
        _create_container_instance(
            run_id=run_id,
            experiment_id=experiment_id,
            container_group_name=container_group_name,
            docker_image=docker_image,
            cpu=cpu,
            memory_gb=memory_gb
        )
        _check_container_state(container_group_name=container_group_name)
        _delete_container_instance(container_group_name=container_group_name)


if __name__ == '__main__':
    main()
