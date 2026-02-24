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


def _create_run_id() -> str:
    date_prefix = date.today().isoformat()
    random_suffix = ''.join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(Config.RUN_ID_LENGTH)
    )

    return f"{date_prefix}-{random_suffix}"


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

    _run_cmd(delete_command)


def _create_container_instance(
        experiment_id: str,
        container_group_name: str,
        docker_image: str,
        cpu: str,
        memory_gb: str,
) -> None:
    run_id = _create_run_id()

    acr_login_server = os.getenv("ACR_LOGIN_SERVER")
    acr_username = os.getenv("ACR_USERNAME")
    acr_password = os.getenv("ACR_PASSWORD")

    startup_command = f"python -m main --script-id {experiment_id} --run-id {run_id}"

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
        "--no-wait"
    ]

    logger.info(f"Creating container group '{container_group_name}' with command '{' '.join(create_command)}'")
    _run_cmd(create_command)


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
                    f"Current container state for '{container_group_name}' is '{state}. Checking again in {timeout} seconds..."
                )

                time.sleep(timeout)


def main() -> None:
    with open(Config.BENCHMARK_FILE) as f:
        benchmark_configuration = yaml.safe_load(f)

    for experiment in benchmark_configuration["experiments"]:
        experiment_id = experiment["id"]
        container_group_name = f"benchmark-{experiment_id}"

        docker_image = experiment["image"]

        cpu = str(experiment["cpu"])
        memory_gb = str(experiment["memory_gb"])

        _delete_container_instance(container_group_name=container_group_name)
        _create_container_instance(
            experiment_id=experiment_id,
            container_group_name=container_group_name,
            docker_image=docker_image,
            cpu=cpu,
            memory_gb=memory_gb
        )
        _check_container_state(container_group_name=container_group_name)


if __name__ == '__main__':
    main()
