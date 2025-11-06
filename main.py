from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import run_pipeline


def main() -> None:
    # SETUP
    initialize_dependencies()

    # OPEN STREET MAP
    run_pipeline()


if __name__ == "__main__":
    main()
