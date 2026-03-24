from abc import ABC, abstractmethod


class ITestDatasetService(ABC):
    @abstractmethod
    def run_pipeline(self) -> str:
        """
        Runs the full test dataset generation pipeline. Downloads and formats OSM and FKB building datasets,
        clips and partitions them by region, conflates the two sources, uploads all assets to blob storage,
        and saves the resulting STAC catalog.
        :return: The release string for the generated dataset
        """
        raise NotImplementedError
