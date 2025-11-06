from src.infra.infrastructure import Containers


def initialize_dependencies() -> None:
    container = Containers()
    container.wire(modules=["src.presentation.entrypoints.release_pipeline"])
