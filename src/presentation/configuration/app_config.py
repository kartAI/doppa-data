from src.infra.infrastructure import Containers


def initialize_dependencies(run_id: str) -> None:
    container = Containers()

    container.config.run_id.from_value(run_id)
    container.wire(
        modules=[
            "src.application.common.monitor",
            "src.presentation.entrypoints.blob_storage_db_scan",
            "src.presentation.entrypoints.db_scan_postgis",
            "src.presentation.entrypoints.bbox_filtering",
        ]
    )
