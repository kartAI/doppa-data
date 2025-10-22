import geopandas as gpd
from azure.storage.blob import BlobServiceClient
from duckdb import DuckDBPyConnection

from src.application.contracts import IBlobStorageService
from src.domain.enums import StorageContainer


class BlobStorageService(IBlobStorageService):
    __db_context: DuckDBPyConnection
    __blob_storage_context: BlobServiceClient

    def __init__(self, db_context: DuckDBPyConnection, blob_storage_context: BlobServiceClient):
        self.__db_context = db_context
        self.__blob_storage_context = blob_storage_context

    def upload_file(self, container_name: StorageContainer) -> bool:
        pass

    def delete_file(self) -> bool:
        pass

    def download_file(self) -> gpd.GeoDataFrame:
        pass
