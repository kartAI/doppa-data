from src.application.contracts import IAzurePricingService
from src.application.dtos import AciPricing, BlobStoragePricing, DatabasePricing


class AzurePricingService(IAzurePricingService):
    """
    Hard-coded Azure Norway East pricing (USD) as of 2026.
    All rates are per-second or per-GB unless noted.

    Sources:
        ACI:      https://azure.microsoft.com/pricing/details/container-instances/
        Storage:  https://azure.microsoft.com/pricing/details/storage/blobs/
        Postgres: https://azure.microsoft.com/pricing/details/postgresql/flexible-server/

    Update these constants when Azure publishes new rates.
    """

    # ------------------------------------------------------------------
    # ACI — Norway East, Linux containers
    # ACI bills per vCPU-second and per GB-second of allocated resources.
    # ------------------------------------------------------------------
    _ACI_VCPU_PER_SECOND: float = 0.0000000309  # $0.0000000309 per vCPU-second
    _ACI_MEMORY_GB_PER_SECOND: float = 0.0000000034  # $0.0000000034 per GB-second
    _ACI_NETWORK_EGRESS_PER_GB: float = 0.08  # $0.08 per GB egress (standard Azure)

    # ------------------------------------------------------------------
    # Blob Storage — LRS, Hot tier, Norway East
    # Operations are priced per 10 000 operations.
    # ------------------------------------------------------------------
    _BLOB_STORAGE_GB_PER_MONTH: float = 0.0194  # $0.0194 per GB/month (LRS Hot)
    _BLOB_READ_PER_10K: float = 0.004  # $0.004  per 10 000 read ops
    _BLOB_WRITE_PER_10K: float = 0.065  # $0.065  per 10 000 write ops
    _BLOB_LIST_PER_10K: float = 0.065  # $0.065  per 10 000 list ops
    _BLOB_INGRESS_PER_GB: float = 0.0  # Free inbound
    _BLOB_EGRESS_PER_GB: float = 0.08  # $0.08 per GB outbound

    # ------------------------------------------------------------------
    # PostgreSQL Flexible Server — Norway East
    # Postgres bills on the vCore SKU uptime regardless of utilization.
    # compute_per_second is the full SKU rate divided to per-second.
    #
    # General Purpose, D4s v3 (4 vCores): ~$0.351/hour → /3600
    # Adjust this if your server uses a different SKU.
    # ------------------------------------------------------------------
    _POSTGRES_COMPUTE_PER_SECOND: float = 0.351 / 3600  # ~$0.0000975/s
    _POSTGRES_STORAGE_GB_PER_MONTH: float = 0.115  # $0.115 per GB/month
    _POSTGRES_EGRESS_PER_GB: float = 0.08  # $0.08 per GB egress

    def get_aci_pricing(self) -> AciPricing:
        return AciPricing(
            vcpu_per_second=self._ACI_VCPU_PER_SECOND,
            memory_gb_per_second=self._ACI_MEMORY_GB_PER_SECOND,
            network_egress_per_gb=self._ACI_NETWORK_EGRESS_PER_GB,
        )

    def get_blob_storage_pricing(self) -> BlobStoragePricing:
        return BlobStoragePricing(
            read_operation_cost=self._BLOB_READ_PER_10K / 10_000,
            write_operation_cost=self._BLOB_WRITE_PER_10K / 10_000,
            list_operation_cost=self._BLOB_LIST_PER_10K / 10_000,
            storage_gb_per_month=self._BLOB_STORAGE_GB_PER_MONTH,
            ingress_per_gb=self._BLOB_INGRESS_PER_GB,
            egress_per_gb=self._BLOB_EGRESS_PER_GB,
        )

    def get_database_pricing(self) -> DatabasePricing:
        return DatabasePricing(
            compute_per_second=self._POSTGRES_COMPUTE_PER_SECOND,
            storage_gb_per_month=self._POSTGRES_STORAGE_GB_PER_MONTH,
            network_egress_per_gb=self._POSTGRES_EGRESS_PER_GB,
        )
