import argparse
from typing import Optional

from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import (
    db_scan_blob_storage, duckdb_bbox_filtering, db_scan_postgis, setup_benchmarking_framework
)


def benchmark_runner() -> None:
    script_id, benchmark_iteration, run_id = _get_args()
    initialize_dependencies(run_id=run_id, benchmark_iteration=benchmark_iteration)

    match script_id:
        case "db-scan-blob-storage":
            db_scan_blob_storage()
            return
        case "db-scan-postgis":
            db_scan_postgis()
            return
        case "duckdb-bbox-filtering":
            duckdb_bbox_filtering()
            return
        case "setup-framework":
            setup_benchmarking_framework()
            return
        case _:
            raise ValueError("Script ID is invalid")


def _get_args() -> tuple[str, int, Optional[str]]:
    parser = argparse.ArgumentParser("doppa-data")
    parser.add_argument(
        "--script-id",
        required=True,
        help="Script identifier. Must one of the specified IDs"
    )

    parser.add_argument(
        "--benchmark-iteration",
        required=True,
        help="Identifier for benchmark iteration. Must be an integer greater than or equal to 1"
    )

    parser.add_argument(
        "--run-id",
        help="Run identifier. Randomly generated and prefixed with today's date"
    )

    args = parser.parse_args()
    return args.script_id, int(args.benchmark_iteration), args.run_id


if __name__ == "__main__":
    benchmark_runner()
