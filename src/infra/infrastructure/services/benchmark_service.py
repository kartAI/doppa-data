import duckdb

from src.application.contracts import IBenchmarkService


class BenchmarkService(IBenchmarkService):
    __duckdb_context: duckdb.DuckDBPyConnection

    def __init__(self, duckdb_context: duckdb.DuckDBPyConnection) -> None:
        self.__duckdb_context = duckdb_context

    def download_parquet_as_shapefile_locally(self, azure_virtual_file_path: str, save_path: Path) -> None:
        save_path.parent.mkdir(parents=True, exist_ok=True)

        self.__duckdb_context.execute(
            f"""
            CREATE TABLE buildings AS SELECT * FROM read_parquet('{azure_virtual_file_path}');
            
            COPY (
                SELECT * FROM buildings
            ) TO '{str(save_path)}'
            WITH (
                FORMAT GDAL,
                DRIVER 'ESRI Shapefile'
            )
            """
        )
