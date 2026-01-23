from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import run_pipeline


def main() -> None:
    initialize_dependencies()
    run_pipeline()


if __name__ == "__main__":
    main()
