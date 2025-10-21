from src.infra.infrastructure import Containers


def initialize_dependencies() -> None:
    container = Containers()
    container.wire(modules=["src.presentation.entrypoints.open_street_map_pipeline"])
