from typing import Any

import pandas as pd

from src.application.contracts import IMonitoringStorageService, IBlobStorageService, IBytesService, IFilePathService
from src.domain.enums import StorageContainer


class MonitoringStorageService(IMonitoringStorageService):
    __blob_storage_service: IBlobStorageService
    __bytes_service: IBytesService
    __file_path_service: IFilePathService

    def __init__(
            self,
            blob_storage_service: IBlobStorageService,
            bytes_service: IBytesService,
            file_path_service: IFilePathService
    ) -> None:
        self.__blob_storage_service = blob_storage_service
        self.__bytes_service = bytes_service
        self.__file_path_service = file_path_service

    def write_metadata_to_blob_storage(self, query_id: str, run_id: str) -> None:
        raise NotImplementedError

    def write_run_to_blob_storage(
            self,
            samples: list[dict[str, Any]],
            query_id: str,
            run_id: str,
            iteration: int
    ) -> None:
        blob_name = self.__file_path_service.create_hive_blob_path(
            file_name="data.parquet",
            query_id=query_id,
            run_id=run_id,
            iteration=iteration
        )

        df = pd.DataFrame(samples)
        df_bytes = self.__bytes_service.convert_df_to_parquet_bytes(df)
        self.__blob_storage_service.upload_file(
            container_name=StorageContainer.BENCHMARKS,
            blob_name=blob_name,
            data=df_bytes
        )
