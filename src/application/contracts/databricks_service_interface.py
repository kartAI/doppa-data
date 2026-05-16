from abc import ABC, abstractmethod

from src.application.dtos import DatabricksRunResult


class IDatabricksService(ABC):
    @abstractmethod
    def submit_and_wait(self, num_workers: int) -> DatabricksRunResult:
        """
        Submit a one-time Databricks run for the national-scale spatial join and block until it
        reaches a terminal state (TERMINATED, SKIPPED, or INTERNAL_ERROR).
        :param num_workers: Number of worker nodes to provision for the cluster.
        :return: DatabricksRunResult with `execution_duration_s`, `cardinality`, and the six Spark
            phase metrics self-reported by the notebook via `dbutils.notebook.exit` JSON.
            `execution_duration_s` measures the spatial join + count() only (excludes cluster
            provisioning, Sedona init, and teardown).
        :rtype: DatabricksRunResult
        :raises RuntimeError: If the run finishes in a non-successful state or the notebook does not
            emit the expected JSON payload.
        """
        raise NotImplementedError
