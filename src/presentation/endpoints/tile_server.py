from contextlib import asynccontextmanager

from dependency_injector.wiring import Provide, inject
from fastapi import FastAPI, HTTPException, Response
from starlette.middleware.cors import CORSMiddleware

from src.application.contracts import IMVTService
from src.infra.infrastructure import Containers
from src.presentation.configuration import initialize_dependencies


@inject
async def _db_call(z: int, x: int, y: int, mvt_service: IMVTService = Provide[Containers.mvt_service]):
    return await mvt_service.get_mvt_tiles(z=z, x=x, y=y)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """FastAPI lifespan context that initializes the DI container before serving requests."""
    initialize_dependencies(run_id="not-needed", benchmark_run=1)
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/tiles/{z}/{x}/{y}")
async def get_tiles(z: int, x: int, y: int):
    """
    HTTP GET ``/tiles/{z}/{x}/{y}``. Returns the buildings MVT tile as
    ``application/x-protobuf`` bytes. Responds 400 for zoom levels outside [0, 22]
    and 404 when no features intersect the tile. Caching headers are disabled.
    :param z: Tile zoom level (0-22).
    :param x: Tile X coordinate.
    :param y: Tile Y coordinate.
    :return: FastAPI Response carrying the MVT tile bytes.
    :rtype: Response
    """
    if z < 0 or z > 22:
        raise HTTPException(status_code=400, detail="Invalid zoom")

    tile = await _db_call(z, x, y)
    if not tile:
        raise HTTPException(status_code=404, detail="Tile not found")

    return Response(
        content=tile,
        media_type="application/x-protobuf",
        headers={
            "Content-Encoding": "identity",
            "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }
    )
