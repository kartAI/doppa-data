from viztracer import VizTracer

from src import Config
from src.presentation.configuration import initialize_dependencies
from src.presentation.entrypoints import run_pipeline


def main() -> None:
    initialize_dependencies()
    if not Config.ENABLE_PROFILING:
        run_pipeline()
        return

    tracer = VizTracer(
        ignore_c_function=True,
        ignore_frozen=True,
    )
    tracer.start()
    run_pipeline()
    tracer.stop()
    tracer.save(str(Config.PROFILE_FILE))


if __name__ == "__main__":
    main()
