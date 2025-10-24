from io import BytesIO

from azure.storage.blob import BlobServiceClient, ContainerClient, PublicAccess
from azure.core.exceptions import ResourceNotFoundError

from src import Config
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
            self.__blob_storage_context.create_container(container_name.value, public_access=PublicAccess.BLOB)
            logger.info(f"Created blob storage container: {container_name.value}")

    def get_container(self, container_name: StorageContainer) -> ContainerClient:
        self.ensure_container(container_name)
        return self.__blob_storage_context.get_container_client(container_name.value)

    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str:
        container = self.get_container(container_name)
        blob_client = container.upload_blob(
            name=blob_name,
            data=data,
            overwrite=True,
            max_concurrency=Config.BLOB_STORAGE_MAX_CONCURRENCY
        )

        logger.info(f"Uploaded blob '{blob_name}'. It can be accessed at: {blob_client.url}")
        return blob_client.url

    def delete_file(self) -> bool:
        pass

    def download_file(self, container_name: StorageContainer, blob_name: str) -> bytes | None:
        try:
            logger.info(f"Downloading bytes from blob '{blob_name}' from container '{container_name.value}'.")
            container = self.get_container(container_name)
            blob_client = container.get_blob_client(blob_name)
            data = blob_client.download_blob().readall()
            return data
        except ResourceNotFoundError:
            logger.warning(f"No blob found with name '{blob_name}' in container '{container_name.value}'.")
            return None
