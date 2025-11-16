from abc import ABC, abstractmethod

import pandas as pd

from src.domain.enums import Theme


class IConflationService(ABC):
    @abstractmethod
    def find_fkb_osm_diff(self, release: str, theme: Theme) -> pd.DataFrame:
        raise NotImplementedError
