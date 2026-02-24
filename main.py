import json
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

    result = subprocess.run(cmd, capture_output=True, text=True, check=False, shell=True)

    if result.returncode != 0:
        cmd_str = " ".join(cmd)
        logger.info(f"Command: {cmd_str}")
        logger.error(result.stderr)
        raise RuntimeError(f"Command failed with exit code {result.returncode}")

    return result.stdout


def main() -> None:
    with open(Config.BENCHMARK_FILE) as f:
        benchmark_configuration = yaml.safe_load(f)

    for experiment in benchmark_configuration["experiments"]:
        experiment_id = experiment["id"]
        docker_image = experiment["image"]
        cpu = str(experiment["cpu"])
        memory_gb = str(experiment["memory_gb"])

        container_group_name = f"benchmark-{experiment_id}"
        run_id = _create_run_id()
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
            "--command-line", startup_command
        ]

        logger.info(f"Creating container group '{container_group_name}' with command '{' '.join(create_command)}'")
        _run_cmd(create_command)

        while True:
            show_command = [
                "az", "container", "show",
                "--resource-group", Config.AZURE_RESOURCE_GROUP,
                "--name", container_group_name,
                "--output", "json"
            ]

            data = json.loads(_run_cmd(show_command))
            state = data["instanceView"]["state"]
            logger.info(f"Container state for '{container_group_name}' is '{state}'")

            if state in ("Succeeded", "Failed"):
                break

            time.sleep(10)


if __name__ == '__main__':
    main()
