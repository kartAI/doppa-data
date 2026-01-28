import re
from datetime import datetime
from typing import Literal

from src.application.contracts import IFilePathService
from src.domain.enums import Theme, StorageContainer


class FilePathService(IFilePathService):
    def create_dataset_blob_path(
            self,
            release: str,
            theme: Theme,
            region: str,
            file_name: str,
            **kwargs
    ) -> str:
        FilePathService.validate_file_path(release=release, region=region, file_name=file_name)
        middle = '/'.join([f'{key}={value}' for key, value in kwargs.items()]) + '/' if kwargs else ''
        return f"release/{release}/{middle}theme={theme.value}/region={region}/{file_name}"

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

        if not (region == "*" or re.fullmatch(r"\d{2}", region)):
            raise AssertionError("region must be two digits (e.g. '03')")

        if not (
                file_name == "*.parquet"
                or re.fullmatch(r"part_\d{5,}\.parquet", file_name)
        ):
            raise AssertionError(
                f"invalid file_name '{file_name}': expected format 'part_00000.parquet' or '*.parquet'"
            )

    @staticmethod
    def get_blob_file_name(file_path: str) -> str:
        file_name = file_path.split("/")[-1]
        return file_name.split(".")[0]

    @staticmethod
    def remove_blob_file_name_from_path(file_path: str) -> str:
        file_name = FilePathService.get_blob_file_name(file_path)
        base_file_path = file_path.removesuffix(file_name)
        return base_file_path

    @staticmethod
    def create_blob_path(*args) -> str:
        return "/".join(args)

    @staticmethod
    def create_virtual_filesystem_path(
            storage_scheme: Literal["az"],
            container: StorageContainer,
            release: str,
            theme: Theme,
            region: str,
            file_name: str,
            **kwargs: str
    ) -> str:
        FilePathService.validate_file_path(release=release, region=region, file_name=file_name)
        middle = '/'.join([f'{key}={value}' for key, value in kwargs.items()]) + "/" if kwargs else ''
        return f"{storage_scheme}://{container.value}/release/{release}/{middle}theme={theme.value}/region={region}/{file_name}"
