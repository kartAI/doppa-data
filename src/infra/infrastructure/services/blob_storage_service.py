import geopandas as gpd
from azure.storage.blob import BlobServiceClient, ContainerClient
from duckdb import DuckDBPyConnection

from src.application.common import logger
from src.application.contracts import IBlobStorageService
from src.domain.enums import StorageContainer


class BlobStorageService(IBlobStorageService):
    __blob_storage_context: BlobServiceClient

    def __init__(self, blob_storage_context: BlobServiceClient):
        self.__blob_storage_context = blob_storage_context

    def ensure_container(self, container_name: StorageContainer) -> None:
        container_client = self.__blob_storage_context.get_container_client(container_name.value)
        if not container_client.exists():
            logger.info(f"Blob storage container '{container_name.value}' does not exist. Creating...")
            self.__blob_storage_context.create_container(container_name.value)
            logger.info(f"Created blob storage container: {container_name.value}")

    def get_container(self, container_name: StorageContainer) -> ContainerClient:
        self.ensure_container(container_name)
        return self.__blob_storage_context.get_container_client(container_name.value)

    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str:
        logger.info(f"Uploading blob '{blob_name}' to container '{container_name.value}'...")
        container = self.get_container(container_name)
        blob_client = container.upload_blob(name=blob_name, data=data)
        logger.info(f"Uploaded blob '{blob_name}'. It can be accessed at: {blob_client.url}")
        return blob_client.url

    def delete_file(self) -> bool:
        pass

    def download_file(self) -> gpd.GeoDataFrame:
        pass
