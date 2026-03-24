from abc import ABC, abstractmethod

from src.application.dtos import BenchmarkConfiguration


class IBenchmarkConfigurationService(ABC):
    @abstractmethod
    def get_experiment_configuration(self, script_id: str) -> BenchmarkConfiguration:
        raise NotImplementedError
