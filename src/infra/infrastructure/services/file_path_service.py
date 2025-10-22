import re
from datetime import datetime

from src.application.contracts import IFilePathService
from src.domain.enums import Theme


class FilePathService(IFilePathService):
    def create_storage_account_file_path(
            self,
            release: str,
            theme: Theme,
            region: str,
            file_name: str,
            *prefix: str
    ) -> str:
        FilePathService.validate_file_path(release=release, region=region, file_name=file_name)

        path: str
        if len(prefix) > 0:
            path = f"{'/'.join(prefix)}/release/{release}/theme={theme.value}/region={region}/{file_name}"
        else:
            path = f"release/{release}/theme={theme.value}/region={region}/{file_name}"

        return path

    @staticmethod
    def validate_file_path(release: str, region: str, file_name: str) -> None:
        """Validate release, region, and file name format."""
        parts = release.rsplit('.', 1)
        if len(parts) != 2:
            raise AssertionError("release must be in format 'yyyy-mm-dd.x'")

        date_part, version_part = parts
        try:
            datetime.strptime(date_part, "%Y-%m-%d")
        except ValueError:
            raise AssertionError("release date must be a valid date in 'yyyy-mm-dd' format")

        if not version_part.isdigit() or int(version_part) < 0:
            raise AssertionError("release version must be a non-negative integer")

        if not re.fullmatch(r"\d{2}", region):
            raise AssertionError("region must be two digits (e.g. '03')")

        if not re.fullmatch(r"part_\d{5,}\.parquet", file_name):
            raise AssertionError(f"invalid file_name '{file_name}': expected format 'part_00000.parquet'")
