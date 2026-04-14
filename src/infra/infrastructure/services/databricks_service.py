import base64
import time
from pathlib import Path

import requests

from src import Config
from src.application.common import logger
from src.application.contracts import IDatabricksService

_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


class DatabricksService(IDatabricksService):
    def __init__(self) -> None:
        pass

    @property
    def _host(self) -> str:
        if not Config.DATABRICKS_HOST:
            raise EnvironmentError(
                "DATABRICKS_HOST is not set. Add it to your .env file."
            )
        return Config.DATABRICKS_HOST.rstrip("/")

    @property
    def _headers(self) -> dict:
        if not Config.DATABRICKS_TOKEN:
            raise EnvironmentError(
                "DATABRICKS_TOKEN is not set. Add it to your .env file."
            )
        return {
            "Authorization": f"Bearer {Config.DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }

    def submit_and_wait(self, num_workers: int) -> float:
        self._upload_notebook()
        run_id = self._submit_run(num_workers)
        logger.info(
            f"Submitted Databricks run {run_id} with {num_workers} worker(s). Polling for completion."
        )
        return self._wait_for_run(run_id)

    def _upload_notebook(self) -> None:
        folder = str(Path(Config.DATABRICKS_WORKSPACE_NOTEBOOK_PATH).parent)
        mkdirs_response = requests.post(
            f"{self._host}/api/2.0/workspace/mkdirs",
            headers=self._headers,
            json={"path": folder},
            timeout=30,
        )
        if not mkdirs_response.ok:
            raise RuntimeError(
                f"Failed to create workspace folder: {mkdirs_response.status_code}: {mkdirs_response.text}"
            )

        content = base64.b64encode(
            Path(Config.DATABRICKS_LOCAL_SCRIPT_PATH).read_bytes()
        ).decode("utf-8")
        response = requests.post(
            f"{self._host}/api/2.0/workspace/import",
            headers=self._headers,
            json={
                "path": Config.DATABRICKS_WORKSPACE_NOTEBOOK_PATH,
                "format": "SOURCE",
                "language": "PYTHON",
                "content": content,
                "overwrite": True,
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Failed to upload notebook to workspace: {response.status_code}: {response.text}"
            )
        logger.info(
            f"Uploaded notebook to '{Config.DATABRICKS_WORKSPACE_NOTEBOOK_PATH}'."
        )

    def _submit_run(self, num_workers: int) -> str:
        payload = {
            "run_name": f"national-scale-spatial-join-{num_workers}-nodes",
            "tasks": [
                {
                    "task_key": "spatial-join",
                    "notebook_task": {
                        "notebook_path": Config.DATABRICKS_WORKSPACE_NOTEBOOK_PATH,
                        "base_parameters": {
                            "account_key": Config.AZURE_BLOB_STORAGE_ACCOUNT_KEY,
                            "account_name": Config.AZURE_BLOB_STORAGE_ACCOUNT_NAME,
                            "release": Config.BENCHMARK_DOPPA_DATA_RELEASE,
                            "municipalities_file": Config.DATABRICKS_MUNICIPALITIES_FILE,
                        },
                    },
                    "new_cluster": {
                        "spark_version": Config.DATABRICKS_SPARK_VERSION,
                        "node_type_id": Config.DATABRICKS_NODE_TYPE_ID,
                        "num_workers": num_workers,
                        "spark_conf": {
                            "spark.driver.memory": Config.DATABRICKS_DRIVER_MEMORY,
                            "spark.driver.memoryOverhead": Config.DATABRICKS_DRIVER_MEMORY_OVERHEAD,
                        },
                    },
                    "libraries": [
                        {
                            "maven": {
                                "coordinates": Config.DATABRICKS_SEDONA_MAVEN_COORDINATES
                            }
                        },
                        {"pypi": {"package": Config.DATABRICKS_SEDONA_PYPI_PACKAGE}},
                        {"pypi": {"package": "geopandas==0.14.4"}},
                    ],
                }
            ],
        }

        response = requests.post(
            f"{self._host}/api/2.1/jobs/runs/submit",
            headers=self._headers,
            json=payload,
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(
                f"Databricks runs/submit failed with {response.status_code}: {response.text}"
            )
        run_id: str = str(response.json()["run_id"])
        return run_id

    def _wait_for_run(self, run_id: str) -> float:
        """Poll until terminal state. Returns execution_duration in seconds (excludes provisioning)."""
        while True:
            response = requests.get(
                f"{self._host}/api/2.1/jobs/runs/get",
                headers=self._headers,
                params={"run_id": run_id},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            state = data.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "")
            result_state = state.get("result_state", "")

            state_msg = f"life_cycle_state={life_cycle_state}"
            if result_state:
                state_msg += f", result_state={result_state}"
            logger.info(f"Run {run_id}: {state_msg}")

            if life_cycle_state in _TERMINAL_STATES:
                if result_state != "SUCCESS":
                    raise RuntimeError(
                        f"Databricks run {run_id} finished with result_state='{result_state}'. "
                        f"State message: {state.get('state_message', '')}"
                    )
                execution_duration_ms = data.get("execution_duration", 0)
                setup_duration_ms = data.get("setup_duration", 0)
                cleanup_duration_ms = data.get("cleanup_duration", 0)
                execution_duration_s = execution_duration_ms / 1000
                logger.info(
                    f"Databricks run {run_id} completed successfully. "
                    f"setup={setup_duration_ms / 1000:.1f}s, "
                    f"execution={execution_duration_s:.1f}s, "
                    f"cleanup={cleanup_duration_ms / 1000:.1f}s"
                )
                return execution_duration_s

            time.sleep(Config.DATABRICKS_POLL_INTERVAL_SECONDS)
