from io import BytesIO

import geopandas as gpd
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient, PublicAccess

from src import Config
from src.application.common import logger
from src.application.contracts import IBlobStorageService, IFilePathService
from src.domain.enums import StorageContainer, Theme


class BlobStorageService(IBlobStorageService):
    __blob_storage_context: BlobServiceClient
    __file_path_service: IFilePathService

    def __init__(self, blob_storage_context: BlobServiceClient, file_path_service: IFilePathService):
        self.__blob_storage_context = blob_storage_context
        self.__file_path_service = file_path_service

    def ensure_container(self, container_name: StorageContainer) -> None:
        container_client = self.__blob_storage_context.get_container_client(container_name.value)
        if not container_client.exists():
            logger.info(f"Blob storage container '{container_name.value}' does not exist. Creating...")
            self.__blob_storage_context.create_container(container_name.value, public_access=PublicAccess.BLOB)
            logger.info(f"Created blob storage container: {container_name.value}")

    def get_container(self, container_name: StorageContainer) -> ContainerClient:
        self.ensure_container(container_name)
        return self.__blob_storage_context.get_container_client(container_name.value)

    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str | None:
        if len(data) == 0:
            return None

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
        raise NotImplementedError

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

    def is_blob_in_storage_container(self, container_name: StorageContainer, blob_name: str) -> bool:
        container = self.get_container(container_name)
        blob_client = container.get_blob_client(blob_name)
        return blob_client.exists()

    def upload_blobs_as_parquet(
            self,
            release: str,
            theme: Theme,
            region: str,
            partitions: list[gpd.GeoDataFrame],
            **kwargs: str
    ) -> list[str]:
        asset_paths = []

        for index, partition in enumerate(partitions):
            if partition.empty:
                logger.info(f"Partition {index} for region '{region}' is empty. Skipping upload.")
                continue

            storage_path = self.__file_path_service.create_dataset_blob_path(
                release=release,
                theme=theme,
                region=region,
                file_name=f"part_{index:05d}.parquet",
                **kwargs if kwargs else {}
            )

            with BytesIO() as buffer:
                partition.to_parquet(
                    buffer,
                    index=False,
                    compression="snappy",
                    geometry_encoding="WKB",
                    schema_version="1.1.0",
                    write_covering_bbox=True
                )

                buffer.seek(0)

                asset_file_path = self.upload_file(
                    container_name=StorageContainer.RAW,
                    blob_name=storage_path,
                    data=buffer.getvalue()
                )

                if asset_file_path:
                    asset_paths.append(asset_file_path)

        return asset_paths
