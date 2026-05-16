from abc import ABC, abstractmethod

from src.domain.enums import DatasetSize


class IDatasetSynthesisService(ABC):
    @abstractmethod
    def run_pipeline(self, release: str, target_size: DatasetSize) -> None:
        """
        Synthesizes a larger building dataset from the existing `size=small` partition for the given
        release by geometric cloning (translate + rotate + jitter). Output is written under
        `release/{release}/size={target_size}/` with the same Hive layout as the source. Only
        `DatasetSize.MEDIUM` and `DatasetSize.LARGE` are valid targets; `DatasetSize.SMALL` is already
        produced by the conflation step and raises `ValueError`.
        :param release: Release identifier on the format 'yyyy-mm-dd.x'.
        :param target_size: Target dataset size enum. Must be MEDIUM or LARGE.
        :return: None
        :raises ValueError: If `target_size` has a non-positive `clones_per_polygon`.
        """
        raise NotImplementedError
