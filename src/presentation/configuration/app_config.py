from src.infra.infrastructure import Containers


def initialize_dependencies(run_id: str, benchmark_run: int) -> None:
    container = Containers()

    container.config.run_id.from_value(run_id)
    container.config.benchmark_run.from_value(benchmark_run)

    container.wire(
        modules=[
            "src.application.common.monitor",
            "src.presentation.entrypoints.db_scan_blob_storage",
            "src.presentation.entrypoints.db_scan_postgis",
            "src.presentation.entrypoints.bbox_filtering",
            "src.presentation.entrypoints.setup_benchmarking_framework",
        ]
    )
