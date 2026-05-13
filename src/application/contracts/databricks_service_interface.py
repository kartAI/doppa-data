from abc import ABC, abstractmethod


class IDatabricksService(ABC):
    @abstractmethod
    def submit_and_wait(self, num_workers: int) -> tuple[float, int]:
        """
        Submit a one-time Databricks run for the national-scale spatial join and block until it reaches
        a terminal state (TERMINATED, SKIPPED, or INTERNAL_ERROR).
        :param num_workers: Number of worker nodes to provision for the cluster.
        :return: Tuple of (elapsed_seconds, cardinality) self-reported by the notebook via dbutils.notebookExit JSON.
            elapsed_seconds measures the spatial join + count() only (excludes cluster provisioning, Sedona init, and teardown).
        :raises RuntimeError: If the run finishes in a non-successful state or the notebook does not emit the expected JSON payload.
        """
        raise NotImplementedError
