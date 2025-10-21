from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import extract_osm_buildings


def main() -> None:
    # SETUP
    initialize_dependencies()

    # OPEN STREET MAP
    extract_osm_buildings()


if __name__ == "__main__":
    main()
