from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class IReleaseService(ABC):
    @abstractmethod
    def get_latest_release(self) -> tuple[date, int, pd.DataFrame] | None:
        raise NotImplementedError

    @abstractmethod
    def create_release(self) -> str:
        raise NotImplementedError
