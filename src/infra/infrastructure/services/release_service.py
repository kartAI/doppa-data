import datetime as dt
from datetime import date, datetime
from io import BytesIO

import pandas as pd

from src import Config
from src.application.contracts import IReleaseService, IBlobStorageService, IBytesService
from src.domain.enums import StorageContainer


class ReleaseService(IReleaseService):
    __blob_storage_service: IBlobStorageService
    __bytes_service: IBytesService

    def __init__(self, blob_storage_service: IBlobStorageService, bytes_service: IBytesService):
        self.__blob_storage_service = blob_storage_service
        self.__bytes_service = bytes_service

    def get_latest_release(self) -> tuple[date, int, pd.DataFrame] | None:
        release_file_bytes = self.__blob_storage_service.download_file(
            StorageContainer.METADATA,
            Config.RELEASE_FILE_NAME
        )

        if release_file_bytes is None:
            return None

        releases = self.__bytes_service.convert_bytes_to_df(release_file_bytes)
        if releases.empty:
            return None

        latest_row = releases.loc[releases["date_created"].idxmax()]
        release_str = latest_row.get("release", "")
        date_part, _, version_part = release_str.rpartition(".")
        if not date_part or not version_part:
            return None

        try:
            parsed_date = date.fromisoformat(date_part)
            version = int(version_part)
            return parsed_date, version, releases
        except (ValueError, TypeError):
            return None

    def create_release(self) -> str:
        release: str
        releases: pd.DataFrame

        latest_release = self.get_latest_release()
        if latest_release is None:
            current_date = date.today()
            version = 0
            release = ReleaseService.__create_release_string(current_date, version)
            releases = pd.DataFrame({"release": [release], "date_created": [datetime.now(dt.UTC)]})
        else:
            latest_date, latest_version, releases = latest_release
            current_date = date.today()
            if current_date > latest_date:
                version = 0
            else:
                version = latest_version + 1
            release = ReleaseService.__create_release_string(current_date, version)
            releases = pd.concat([
                releases,
                pd.DataFrame({"release": [release], "date_created": [datetime.now(dt.UTC)]})
            ], ignore_index=True)

        buffer = BytesIO()
        releases.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.__blob_storage_service.upload_file(
            StorageContainer.METADATA,
            Config.RELEASE_FILE_NAME,
            buffer.read()
        )

        return release

    @staticmethod
    def __create_release_string(release_date: date, version: int) -> str:
        return f"{release_date.isoformat()}.{version}"
