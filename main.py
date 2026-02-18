import uuid

from src.application.common import monitor_cpu_and_ram
from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import run_pipeline

RUN_ID = str(uuid.uuid4())


@monitor_cpu_and_ram(run_id=RUN_ID, query_id="osm-fkb-conflation")
def main() -> None:
    initialize_dependencies()
    run_pipeline()


if __name__ == "__main__":
    main()
