from abc import abstractmethod, ABC

from src.application.dtos import AciPricing, BlobStoragePricing, DatabasePricing, DatabricksPricing


class IAzurePricingService(ABC):

    @abstractmethod
    def get_aci_pricing(self) -> AciPricing:
        """
        Returns the Azure Container Instance pricing for the configured region and tier. Values are
        per-second for vCPU and memory, and per-GB for network egress.
        :return: AciPricing DTO with vCPU per second, memory GB per second, and network egress per GB.
        :rtype: AciPricing
        """
        raise NotImplementedError

    @abstractmethod
    def get_blob_storage_pricing(self) -> BlobStoragePricing:
        """
        Returns the blob storage pricing for the configured region, redundancy, and tier. Operation costs
        are per single operation (already divided from the per-10 000 list price). Storage is per GB/month.
        :return: BlobStoragePricing DTO with read/write/list operation costs, monthly storage cost, and
            ingress/egress per GB.
        :rtype: BlobStoragePricing
        """
        raise NotImplementedError

    @abstractmethod
    def get_database_pricing(self) -> DatabasePricing:
        """
        Returns the PostgreSQL Flexible Server pricing for the configured SKU and region.
        :return: DatabasePricing DTO with compute per second, storage per GB/month, and network egress per GB.
        :rtype: DatabasePricing
        """
        raise NotImplementedError

    @abstractmethod
    def get_databricks_pricing(self) -> DatabricksPricing:
        """
        Returns the Azure Databricks pricing for the configured tier, region, and node type. The total
        compute cost per node-hour is `(dbu_per_node_per_hour * dbu_price_per_hour) + vm_cost_per_node_per_hour`.
        :return: DatabricksPricing DTO with DBU rate per node-hour, DBU price per hour, VM cost per
            node-hour, and network egress per GB.
        :rtype: DatabricksPricing
        """
        raise NotImplementedError
