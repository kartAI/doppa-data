from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class IReleaseService(ABC):
    @abstractmethod
    def get_latest_release(self) -> tuple[date, int, pd.DataFrame] | None:
        """
        Returns the latest release recorded in the release metadata blob. The release file is read from
        blob storage and parsed for the row with the most recent `date_created`.
        :return: Tuple containing the release date, version number, and the full releases DataFrame.
            Returns None if no release file exists, the file is empty, or the latest entry cannot be parsed.
        :rtype: tuple[date, int, pd.DataFrame] | None
        """
        raise NotImplementedError

    @abstractmethod
    def create_release(self) -> str:
        """
        Creates a new release and persists it to the release metadata blob. The version number is
        incremented when a release already exists for the current date, otherwise it resets to 0.
        :return: New release identifier on the format 'yyyy-mm-dd.x'.
        :rtype: str
        """
        raise NotImplementedError
