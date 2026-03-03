from pathlib import Path

import duckdb

from src.application.contracts import IBenchmarkService


class BenchmarkService(IBenchmarkService):
    __duckdb_context: duckdb.DuckDBPyConnection

    def __init__(self, duckdb_context: duckdb.DuckDBPyConnection) -> None:
        self.__duckdb_context = duckdb_context

    def download_parquet_as_shapefile_locally(self, virtual_file_path: str, save_path: Path) -> None:
        save_path.parent.mkdir(parents=True, exist_ok=True)

        base = save_path.with_suffix("")
        for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
            candidate = base.with_suffix(ext)
            candidate.unlink(missing_ok=True)

        self.__duckdb_context.execute(
            f"""
            CREATE  TABLE buildings AS SELECT
                *,
                bbox.maxx AS bbox_maxx,
                bbox.maxy AS bbox_maxy,
                bbox.minx AS bbox_minx,
                bbox.miny AS bbox_miny,
                bbox.xmax AS bbox_xmax,
                bbox.xmin AS bbox_xmin,
                bbox.ymax AS bbox_ymax,
                bbox.ymin AS bbox_ymin
            FROM read_parquet('{virtual_file_path}');

            COPY (
                SELECT
                    * EXCLUDE (bbox)
                FROM buildings
            ) TO '{save_path.as_posix()}'
            WITH (
                FORMAT GDAL,
                DRIVER 'ESRI Shapefile'
            );
            """
        )
