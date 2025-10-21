import geopandas as gpd
from duckdb import DuckDBPyConnection

from src.application.contracts import IBlobStorageService


class BlobStorageService(IBlobStorageService):
    __db_context: DuckDBPyConnection

    def __init__(self, db_context: DuckDBPyConnection):
        self.__db_context = db_context

    def upload_file(self) -> bool:
        pass

    def delete_file(self) -> bool:
        pass

    def download_file(self) -> gpd.GeoDataFrame:
        pass
