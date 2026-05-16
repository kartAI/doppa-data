from azure.storage.blob import BlobServiceClient

from src import Config


def create_blob_storage_context() -> BlobServiceClient:
    """
    Creates an Azure `BlobServiceClient` from the connection string configured for the project.
    :return: Azure `BlobServiceClient` bound to the configured storage account.
    :rtype: BlobServiceClient
    """
    blob_storage_context = BlobServiceClient.from_connection_string(Config.AZURE_BLOB_STORAGE_CONNECTION_STRING)
    return blob_storage_context
