from duckdb import DuckDBPyConnection

from src.application.common import logger
from src.application.contracts import (
    IDatasetSynthesisService,
    IFilePathService,
    IBlobStorageService,
    ICountyService,
)
from src.domain.enums import DatasetSize

CLONES_PER_POLYGON: dict[DatasetSize, int] = {
    DatasetSize.MEDIUM: 7,
    DatasetSize.LARGE: 19,
}


class DatasetSynthesisService(IDatasetSynthesisService):
    __db_context: DuckDBPyConnection
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService
    __county_service: ICountyService

    def __init__(
        self,
        db_context: DuckDBPyConnection,
        file_path_service: IFilePathService,
        blob_storage_service: IBlobStorageService,
        county_service: ICountyService,
    ):
        self.__db_context = db_context
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service
        self.__county_service = county_service

    def run_pipeline(self, release: str, target_size: DatasetSize) -> None:
        if target_size not in CLONES_PER_POLYGON:
            raise ValueError(
                f"DatasetSynthesisService only supports MEDIUM and LARGE. Got '{target_size.value}'."
            )

        clones_per_polygon = CLONES_PER_POLYGON[target_size]
        regions = self.__county_service.get_county_ids()

        logger.info(
            f"Synthesizing '{target_size.value}' dataset for release '{release}' with {clones_per_polygon} clones per source polygon across {len(regions)} regions."
        )

        for region in regions:
            logger.info(
                f"Synthesizing region '{region}' for size '{target_size.value}'..."
            )
            self.__synthesize_region(
                release=release,
                region=region,
                target_size=target_size,
                clones_per_polygon=clones_per_polygon,
            )

        logger.info(
            f"Synthesis of '{target_size.value}' dataset complete for release '{release}'."
        )

    def __synthesize_region(
        self,
        release: str,
        region: str,
        target_size: DatasetSize,
        clones_per_polygon: int,
    ) -> None:
        raise NotImplementedError(
            "Per-region synthesis is implemented in developer tasks 5-8."
        )
