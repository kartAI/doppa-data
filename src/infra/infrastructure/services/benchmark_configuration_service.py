import yaml

from src import Config
from src.application.contracts import IBenchmarkConfigurationService
from src.application.dtos import BenchmarkConfiguration


class BenchmarkConfigurationService(IBenchmarkConfigurationService):
    def __init__(self) -> None:
        self.__experiments_by_id = self.__load_experiments()

    def get_experiment_configuration(self, script_id: str) -> BenchmarkConfiguration:
        experiment = self.__experiments_by_id.get(script_id)
        if experiment is None:
            raise ValueError(f"Script '{script_id}' was not found in configuration")

        return experiment

    @staticmethod
    def __load_experiments() -> dict[str, BenchmarkConfiguration]:
        with Config.BENCHMARK_FILE.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        experiments = data.get("experiments", [])
        experiments_by_id: dict[str, BenchmarkConfiguration] = {}

        for experiment in experiments:
            experiment_configuration = BenchmarkConfiguration(
                id=experiment["id"],
                image=experiment["image"],
                cpu=float(experiment["cpu"]),
                memory_gb=float(experiment["memory_gb"])
            )

            experiments_by_id[experiment_configuration.id] = experiment_configuration
        return experiments_by_id
