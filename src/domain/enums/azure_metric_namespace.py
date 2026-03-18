from enum import Enum


class AzureMetricNamespace(Enum):
    BLOB_STORAGE = "Microsoft.Storage/storageAccounts"
    CONTAINER_INSTANCES = "Microsoft.ContainerInstances/containerGroups"
    POSTGRESQL_FLEXIBLE = "Microsoft.DBforPostgreSQL/flexibleServers"
    WEB_APP = "Microsoft.Web/sites"
