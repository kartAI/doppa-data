from abc import ABC, abstractmethod


class IDatabricksService(ABC):
    @abstractmethod
    def submit_and_wait(self, num_workers: int) -> None:
        """
        Submit a one-time Databricks run for the national-scale spatial join and block until it reaches
        a terminal state (TERMINATED, SKIPPED, or INTERNAL_ERROR).
        :param num_workers: Number of worker nodes to provision for the cluster.
        :return: None
        :raises RuntimeError: If the run finishes in a non-successful state.
        """
        raise NotImplementedError
