import argparse
from typing import Optional

from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import blob_storage_db_scan, duckdb_bbox_filtering, db_scan_postgis


def benchmark_runner() -> None:
    script_id, run_id = get_args()
    initialize_dependencies(run_id=run_id)

    match script_id:
        case "blob-storage-db-scan":
            blob_storage_db_scan()
            return
        case "db-scan-postgis":
            db_scan_postgis()
            return
        case "duckdb-bbox-filtering":
            duckdb_bbox_filtering()
            return
        case _:
            raise ValueError("Script ID is invalid")


def get_script_id() -> str:
    parser = argparse.ArgumentParser("doppa-data")
    parser.add_argument("id", help="ID of script to run")
    args = parser.parse_args()
    return args.id


def get_args() -> tuple[str, Optional[str]]:
    parser = argparse.ArgumentParser("doppa-data")
    parser.add_argument("--script-id", required=True, help="Script identifier. Must one of the specified IDs")
    parser.add_argument("--run-id", help="Run identifier. Randomly generated and prefixed with today's date")
    args = parser.parse_args()
    return args.script_id, args.run_id


if __name__ == "__main__":
    benchmark_runner()
