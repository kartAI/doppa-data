from sqlalchemy import Engine, text

from src.application.contracts import IMVTService


class MVTService(IMVTService):
    __db_context: Engine

    def __init__(self, db_context: Engine):
        self.__db_context = db_context

    def get_mvt_tiles(self, z: int, x: int, y: int) -> bytes | None:
        query = text(
            """
            WITH
                tile_bounds AS (
                    SELECT ST_TileEnvelope(:z, :x, :y) AS geom_3857
                ),
                bounds_4326 AS (
                    SELECT ST_Transform(geom_3857, 4326) AS geom
                    FROM tile_bounds
                ),
                mvtgeom AS (
                    SELECT
                        ST_AsMVTGeom(
                            ST_Transform(buildings.geometry, 3857),
                            tile_bounds.geom_3857,
                            4096,
                            256,
                            true
                        ) AS geometry
                    FROM buildings, tile_bounds, bounds_4326
                    WHERE ST_Intersects(buildings.geometry, bounds_4326.geom)
                )
            SELECT ST_AsMVT(mvtgeom, 'buildings', 4096, 'geometry') AS tile
            FROM mvtgeom
            """
        )

        with self.__db_context.connect() as conn:
            result = conn.execute(query, {"z": z, "x": x, "y": y}).fetchone()

        if result is None or result[0] is None or len(result[0]) == 0:
            return None

        return bytes(result[0])
