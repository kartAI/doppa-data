import re
from datetime import datetime

from src.application.contracts import IFilePathService
from src.domain.enums import Theme


class FilePathService(IFilePathService):
    def create_storage_account_file_path(self, release: str, theme: Theme, region: str, file_name: str) -> str:
        FilePathService.validate_file_path(release=release, file_name=file_name)
        return f"release/{release}/theme={theme.value}/region={region}/{file_name}"

    @staticmethod
    def validate_file_path(release: str, file_name: str) -> None:
        try:
            date_part, version_part = release.rsplit('.', 1)
        except ValueError:
            raise AssertionError("release must be in format yyyy-mm-dd.x")

        try:
            datetime.strptime(date_part, "%Y-%m-%d")
        except ValueError:
            raise AssertionError("release date must be a valid date in yyyy-mm-dd format")

        if not version_part.isdigit():
            raise AssertionError("release version must be a non-negative integer")
        if int(version_part) < 0:
            raise AssertionError("release version must be >= 0")

        if not re.match(r'^part_\d{5,}\.parquet$', file_name):
            raise AssertionError(f"invalid file_name '{file_name}': expected format 'part_00000.parquet'")
