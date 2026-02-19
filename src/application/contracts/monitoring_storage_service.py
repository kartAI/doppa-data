from abc import ABC, abstractmethod


class IMonitoringStorageService(ABC):
    @abstractmethod
    def write_metadata_to_blob_storage(self, query_id: str, run_id: str) -> None:
        """
        Save metadata to blob storage. Metadata includes information about the query and the run, such as query text,
        parameters, execution time, etc. The metadata is saved with a Hive compatible partition structure to allow for
        efficient querying and analysis of the data. The partition structure is defined as follows:
        `query_id=<query_id>/run_id=<run_id>/metadata.json`. This structure allows for easy retrieval of metadata based
        on query ID and run ID.
        :param query_id: Query ID associated with the run which is passed from the main method
        :param run_id: A unique identifier for the run
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def write_run_to_blob_storage(self, query_id: str, run_id: int, iteration: int) -> None:
        """
        Save the run to blob storage. A run includes the samples collected during the execution of the query.
        The run is saved with a Hive compatible partition structure to allow for efficient querying and analysis of the data.
        The partition structure is defined as follows: `query_id=<query_id>/run_id=<run_id>/iteration=<iteration>/<filename>.json`.
        This structure allows for easy retrieval of runs based on query ID, run ID, and iteration number.
        :param query_id: Query ID associated with the run which is passed from the main method
        :param run_id: A unique identifier for the run which is passed from the main method.
        :param iteration: Iteration number of the run which is passed from the main method. This is used to differentiate between multiple runs of the same query.
        :return:
        """
        raise NotImplementedError
