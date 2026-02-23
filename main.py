import argparse

from src.application.common import monitor_cpu_and_ram
from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import run_pipeline, blob_storage_db_scan


@monitor_cpu_and_ram(query_id="main")
def main() -> None:
    initialize_dependencies()
    script_id = get_script_id()

    match script_id:
        case "conflation-pipeline":
            run_pipeline()
            return
        case "blob-storage-db-scan":
            blob_storage_db_scan()
            return
        case _:
            raise ValueError("Script ID is invalid")


def get_script_id() -> str:
    parser = argparse.ArgumentParser("doppa-data")
    parser.add_argument("id", help="ID of script to run")
    args = parser.parse_args()
    return args.id


if __name__ == "__main__":
    main()
