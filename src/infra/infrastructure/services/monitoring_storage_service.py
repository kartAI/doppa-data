from src.application.contracts import IMonitoringStorageService, IBlobStorageService, IBytesService


class MonitoringStorageService(IMonitoringStorageService):
    __blob_storage_service: IBlobStorageService
    __bytes_service: IBytesService

    def __init__(self, blob_storage_service: IBlobStorageService, bytes_service: IBytesService) -> None:
        self.__blob_storage_service = blob_storage_service
        self.__bytes_service = bytes_service

    def write_metadata_to_blob_storage(self, query_id: str, run_id: str) -> None:
        pass

    def write_run_to_blob_storage(self, query_id: str, run_id: int, iteration: int) -> None:
        pass
