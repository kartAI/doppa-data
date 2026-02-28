from src.infra.infrastructure import Containers


def initialize_dependencies(run_id: str, benchmark_iteration: int) -> None:
    container = Containers()

    container.config.run_id.from_value(run_id)
    container.config.benchmark_iteration.from_value(benchmark_iteration)

    container.wire(
        modules=[
            "src.application.common.monitor",
            "src.presentation.entrypoints.db_scan_blob_storage",
            "src.presentation.entrypoints.db_scan_postgis",
            "src.presentation.entrypoints.bbox_filtering",
            "src.presentation.entrypoints.setup_benchmarking_framework",
        ]
    )
