from src.infra.infrastructure import Containers


def initialize_dependencies(run_id: str, benchmark_run: int) -> None:
    container = Containers()

    container.config.run_id.from_value(run_id)
    container.config.benchmark_run.from_value(benchmark_run)

    container.wire(
        modules=[
            "src.application.common.monitor_utils",
            "src.presentation.entrypoints.db_scan_blob_storage",
            "src.presentation.entrypoints.db_scan_postgis",

            "src.presentation.entrypoints.bbox_filtering_advanced_duckdb",
            "src.presentation.entrypoints.bbox_filtering_advanced_postgis",

            "src.presentation.entrypoints.bbox_filtering_simple_local",
            "src.presentation.entrypoints.bbox_filtering_simple_blob_storage",

            "src.presentation.entrypoints.vector_tiles_single_tile_pmtiles",

            "src.presentation.entrypoints.setup_benchmarking_framework",

            "src.presentation.endpoints.tile_server"
        ]
    )
