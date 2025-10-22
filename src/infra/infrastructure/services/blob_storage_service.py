import geopandas as gpd
from azure.storage.blob import BlobServiceClient, ContainerClient
from duckdb import DuckDBPyConnection

from src.application.contracts import IBlobStorageService
from src.domain.enums import StorageContainer


class BlobStorageService(IBlobStorageService):
    __db_context: DuckDBPyConnection
    __blob_storage_context: BlobServiceClient

    def __init__(self, db_context: DuckDBPyConnection, blob_storage_context: BlobServiceClient):
        self.__db_context = db_context
        self.__blob_storage_context = blob_storage_context

    def get_container(self, container_name: StorageContainer) -> ContainerClient:
        return self.__blob_storage_context.get_container_client(container_name.value)

    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str:
        container = self.get_container(container_name)
        blob_client = container.upload_blob(name=blob_name, data=data)
        return blob_client.url

    def delete_file(self) -> bool:
        pass

    def download_file(self) -> gpd.GeoDataFrame:
        pass
