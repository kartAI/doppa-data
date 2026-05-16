from abc import ABC, abstractmethod

from src.application.dtos import BenchmarkConfiguration


class IBenchmarkConfigurationService(ABC):
    @abstractmethod
    def get_experiment_configuration(self, script_id: str) -> BenchmarkConfiguration:
        """
        Returns the benchmark configuration for the given script ID. Configurations are loaded from the
        benchmark YAML file and contain the container image, vCPU count and memory allocation used when
        running the experiment as an Azure Container Instance.
        :param script_id: Script identifier matching an `id` field under `experiments` in the benchmark YAML file.
        :return: BenchmarkConfiguration for the given script ID.
        :rtype: BenchmarkConfiguration
        :raises ValueError: If no experiment with the given script ID exists in the configuration file.
        """
        raise NotImplementedError
