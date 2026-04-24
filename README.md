# doppa: A Framework for Comparing Traditional & CNG Queries

doppa is a reproducible benchmarking framework for evaluating traditional geospatial query stacks
(PostGIS, shapefiles) against cloud-native geospatial (CNG) alternatives (DuckDB over GeoParquet in
blob storage, PMTiles/MVT vector tiles, and Apache Sedona on Databricks) across a range of real-world
spatial query patterns: database scans, bounding-box filtering at varying result-set sizes, vector
tile fetching, spatial aggregation over grids, attribute + spatial compound filters, ordered range
queries, point-in-polygon lookups, and national-scale spatial joins.

Each query is packaged as an independent container image, executed on Azure Container Instances via
an orchestrator, and produces cost and runtime metrics written back to blob storage for downstream
analysis. The framework is designed to make the trade-offs between traditional and CNG approaches
measurable and reproducible on identical datasets and hardware.

<div align="center">

[![Push containers to Azure Container Registry](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml/badge.svg)](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml) [![Publish APIs](https://github.com/kartAI/doppa-data/actions/workflows/publish-api.yml/badge.svg)](https://github.com/kartAI/doppa-data/actions/workflows/publish-api.yml) [![Run Benchmarks](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml/badge.svg?event=schedule)](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml)

</div>

## Table of contents

- [Setup](#setup)
    - [Azure Resources](#azure-resources)
        - [Resource group](#resource-group)
        - [Blob storage](#blob-storage)
        - [User-Assigned Managed Identity (UAMI)](#user-assigned-managed-identity-uami)
        - [Container registry](#container-registry)
        - [PostgreSQL database](#postgresql-database)
        - [Web app for containers](#web-app-for-containers)
        - [Databricks](#databricks)
    - [GitHub Actions](#github-actions)
    - [Local development](#local-development)

## Setup

### Azure Resources

This project utilizes several Azure resources. Some are created and deleted during runtime, whilst others have to be
created manually. This section will give a brief walkthrough on the resources that have to be configured and how to do
so.

> [!NOTE]
> To ensure fair benchmarks set up all resources in Norway East

#### Resource group

Start by creating a resource group named `doppa`. Ensure that you can configure Kubernetes and Databricks with your
current subscription and roles.

#### Blob storage

Blob storage is an essential part of this benchmarking framework. Everything from benchmarking results to the actual
datasets are stored here. Create a storage account named `doppabs`. Under *Data protection* disable everything.
All other settings can be left as default.

> [!IMPORTANT]
> Disabling all data protection options (soft delete for blobs and containers) is required. If these features are
> enabled the Databricks ABFS driver will fail with a 409 error when reading data via the `dfs.core.windows.net`
> endpoint.

There is no need to create the containers
as these are created during runtime. Each container is created with the `Container` access level. If you wish to make
this stricter make the following changes in the `ensure_container` function in [
`BlobStorageService`](./src/infra/infrastructure/services/blob_storage_service.py).

```python
# Public container access
self.__blob_storage_context.create_container(container_name.value, public_access=PublicAccess.CONTAINER)  
```

```python
# Private container access
self.__blob_storage_context.create_container(container_name.value, public_access=PublicAccess.BLOB)
```

#### User-Assigned Managed Identity (UAMI)

To provide the correct access to Azure resources when running the script from GitHub Actions a UAMI has to be
configured. The Actions will sign in to Azure and execute the scripts using the UAMI. Create a UAMI named
`doppa-uami` and navigate to the *Federated credentials* setting. Create two federated credentials with the
following setup:

<div style="display:flex; justify-content:center; align-items:flex-start; gap:20px;">
  <img src=".github/docs/img/github-ci-fc-main.png" width="45%" />
  <img src=".github/docs/img/github-ci-fc-pr.png" width="45%" />
</div>

Change the fields according to your setup.

The next step is to give the UAMI a `Contributor` in the resource group. Navigate to the *Azure role assignments*
setting and press *Add role assignment*. Select the scope `Resource group` and then the resource group `doppa`. Pick the
role `Contributor` and press *Save*.

To view the `AZURE_UAMI_RESOURCE_ID` (needed for later) run the following command:

```powershell
az identity show -g doppa -n doppa-uami --query id -o tsv
```

#### Container registry

Create a container registry named `doppaacr`. The Docker images will be saved here. To ensure that the Actions are able
to pull the images give the UAMI created in the last step a `AcrPull` and `AcrPush` role. In the `doppaacr` resource
navigate to

*Access control (IAM)* and press *Add* > *Add role assignment*. Select the role `AcrPull` and continue. On the next
screen select *Managed identity* under *Assign access to*, and select the `doppa-uami` UAMI under *Members*.
Navigate to the last step and press *Create*.

Repeat the same steps for the `AcrPush` role.

#### PostgreSQL database

Create
an [Azure database for PostgreSQL](https://portal.azure.com/#view/Microsoft_Azure_Marketplace/GalleryItemDetailsBladeNopdl/id/Microsoft.PostgreSQLServer/selectionMode~/false/resourceGroupId//resourceGroupLocation//dontDiscardJourney~/false/selectedMenuId/home/launchingContext~/%7B%22galleryItemId%22%3A%22Microsoft.PostgreSQLServer%22%2C%22source%22%3A%5B%22GalleryFeaturedMenuItemPart%22%2C%22VirtualizedTileDetails%22%5D%2C%22menuItemId%22%3A%22home%22%2C%22subMenuItemId%22%3A%22Search%20results%22%2C%22telemetryId%22%3A%22a28a8a60-8a59-43fd-8def-fc6cba1ca11f%22%7D/searchTelemetryId/0a3db32e-00e8-4ba8-b921-45bc0a7a5a28)
with the following configuration:

Under *Basics*:

- Server name: `doppa-db`
- Region: `Norway East`
- Workload type: `Production`
- Compute + Storage: Disable `Geo-Redundancy`
    - Cluster: Set *Cluster options* to `Server`
    - Compute: Set *Compute tier* to `Burstable` and set *Compute size* to `Standard_B2ms`
    - Storage: Set *Storage type* to `Premium SSD`, *Storage size* to `128 GiB` and *Performance tier* to `P10`
    - Business critical: Set *Zonal resiliency* to `Disabled`
- Authentication method: `PostgreSQL authentication only`

Under *Networking*:

- Firewall rules: Check the box *Allow public access from any Azure service within Azure to this server*.
- Add current IP address to Firewall rules

Navigate to *Review and create* and create the resource.

After the database has been deployed navigate to the setting *Server parameters* and search for `azure.extensions`. In
the drop-down menu select `POSTGIS`. This enables the script to install PostGIS automatically during run-time. Under the
same setting change the following:

- `shared_buffers`: `2097152`
- `effective_cache_size`: `6291456`
- `work_mem`: `65536`

#### Web app for containers

Create
a [web app for containers](https://portal.azure.com/#view/Microsoft_Azure_Marketplace/GalleryItemDetailsBladeNopdl/id/Microsoft.AppSvcLinux/selectionMode~/false/resourceGroupId//resourceGroupLocation//dontDiscardJourney~/false/selectedMenuId/home/launchingContext~/%7B%22galleryItemId%22%3A%22Microsoft.AppSvcLinux%22%2C%22source%22%3A%5B%22GalleryFeaturedMenuItemPart%22%2C%22VirtualizedTileDetails%22%5D%2C%22menuItemId%22%3A%22home%22%2C%22subMenuItemId%22%3A%22Search%20results%22%2C%22telemetryId%22%3A%22135c4e97-6a92-446e-aa0a-3f2201ddfdb1%22%7D/searchTelemetryId/c154ee0a-06d6-49e4-a17f-3820937e6335)
The process is the same for each of the following API servers:

- `doppa-vmt`

Under *Basics*:

- Resource group: `doppa`
- Name: `<name-from-list-above>`
- Publish: `Container`
- Operating system: `Linux`
- Pricing plan: `Premium V4 P0V4`

Under *Container*:

- Image source: `Azure Container Registry`
- Registry: `doppaacr`
- Authentication: `Managed identity`
- Identity: `doppa-uami`
- Image: `<select the image that matches with the name>`
- Tag: `latest`
- Startup command `uvicorn src.presentation.endpoints.<API server script>:app --host 0.0.0.0 --port 8000`

Navigate to *Review + create* and create the resource. Repeat this process for each name in the list.

#### Databricks

The national-scale spatial join benchmarks run on Azure Databricks using Apache Sedona. A separate Databricks
workspace is required.

> [!NOTE]
> Norway East often has insufficient `Standard_D4s_v3` quota for multi-node clusters. Create the workspace in
> **Sweden Central** instead. Cross-region data access (blob storage in Norway East, compute in Sweden Central)
> adds minor latency but does not affect benchmark validity.

##### 1. Request vCPU quota

The benchmarks run clusters with 2, 4, and 8 worker nodes. Each `Standard_D4s_v3` node uses 4 vCPUs, so the
8-node cluster requires 32 vCPUs (plus the driver). The default quota in most regions is 10 vCPUs.

To request a quota increase:

1. Navigate to the [Azure Portal](https://portal.azure.com) → **Subscriptions** → your subscription →
   **Settings** → **Usage + quotas**
2. Filter by region (e.g. Sweden Central) and search for `Standard DSv3 Family vCPUs`
3. Click the pencil icon and request at least **40 vCPUs**
4. Provide a justification (e.g. "Running distributed Spark benchmarks") and submit

Quota increases for small VM families are typically approved automatically within minutes.

##### 2. Create the workspace

1. In the Azure Portal create a new **Azure Databricks** resource
2. Under *Basics*:
   - Resource group: `doppa`
   - Workspace name: `doppa-databricks`
   - Region: **Sweden Central** (or another region with sufficient quota)
   - Pricing tier: `Premium`
3. Leave all other settings as default and press *Review + create*

##### 3. Generate an access token

1. Open the Databricks workspace
2. Navigate to your user icon (top right) → **Settings** → **Developer** → **Access tokens**
3. Click *Generate new token*, give it a name and a suitable expiry, and copy the token value

##### 4. Environment variables

Add the following to your `.env` file:

```dotenv
DATABRICKS_HOST=https://<workspace-id>.azuredatabricks.net
DATABRICKS_TOKEN=<personal-access-token>
AZURE_BLOB_STORAGE_ACCOUNT_KEY=<storage-account-access-key>
```

- `DATABRICKS_HOST`: the full URL of your workspace (visible in the browser address bar after opening the workspace)
- `DATABRICKS_TOKEN`: the token generated in the previous step
- `AZURE_BLOB_STORAGE_ACCOUNT_KEY`: found in Azure Portal → Storage account `doppabs` → **Security + networking** → **Access keys** → copy either `key1` or `key2`

The notebook script is automatically uploaded to the Databricks workspace at run time. No manual upload is required.

##### 5. Upload county boundaries

The spatial join requires Norwegian county polygon data in the `metadata` blob storage container.
Upload `counties.parquet` to the `metadata` container in the `doppabs` storage account before running
the Databricks benchmarks. This can be done via the Azure Portal Storage Browser or the Azure CLI:

```bash
az storage blob upload \
  --account-name doppabs \
  --container-name metadata \
  --name counties.parquet \
  --file <path-to-counties.parquet>
```

### GitHub Actions

In your repository navigate to *Secrets and variables* under *Settings*. Add the following **secrets**:

- `AZURE_UAMI_RESOURCE_ID`
- `AZURE_BLOB_STORAGE_CONNECTION_STRING`
- `AZURE_BLOB_STORAGE_ACCOUNT_KEY`
- `POSTGRES_USERNAME`
- `POSTGRES_PASSWORD`
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

and add the following **variables**:

- `ACR_NAME`
- `ACR_LOGIN_SERVER`
- `AZURE_BLOB_STORAGE_BENCHMARK_CONTAINER`
- `AZURE_BLOB_STORAGE_METADATA_CONTAINER`
- `AZURE_CLIENT_ID`
- `AZURE_RESOURCE_GROUP`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_TENANT_ID`
- `POSTGRES_SERVER_NAME`

These values can be found under the Azure resources previously created. The workflows should now work!

### Local development

> [!NOTE]
> This does not run fully locally, so ensure that all the Azure resources have been configured

Clone the repository from [GitHub](https://github.com/kartAI/doppa-data) and navigate to the project root.

```powershell
git clone https://github.com/kartAI/doppa-data.git
cd doppa-data
```

Create a virtual environment and install the dependencies in the [requirements](./requirements.txt)-file.

```powershell
python -m venv venv                 # Create virtual environment
./venv/Scripts/activate             # Activate venv
pip install -r requirements.txt     # Install dependencies
```

Add the following `.env` file to the project root directory. Swap out the values enclosed by `<>` with the actual
secrets. The containers `dev-benchmarks` and `dev-metadata` ensure that results from the test runs do not disrupt
results from actual runs.

```dotenv
AZURE_SUBSCRIPTION_ID=<azure-subscription-id>
AZURE_UAMI_RESOURCE_ID=<azure-user-assigned-managed-identity-resource-id>

AZURE_BLOB_STORAGE_CONNECTION_STRING=<azure-blob-storage-connection-string>
AZURE_BLOB_STORAGE_ACCOUNT_KEY=<azure-blob-storage-account-key>
AZURE_BLOB_STORAGE_BENCHMARK_CONTAINER=dev-benchmarks
AZURE_BLOB_STORAGE_METADATA_CONTAINER=dev-metadata

ACR_LOGIN_SERVER=<azure-container-registry-login-server>

POSTGRES_SERVER_NAME=<postgres-server-name>
POSTGRES_USERNAME=<postgres-username>
POSTGRES_PASSWORD=<postgres-password>

DATABRICKS_HOST=https://<workspace-id>.azuredatabricks.net
DATABRICKS_TOKEN=<personal-access-token>
```

To run the entire script simply run `python main.py` or `python -m main` and to run a single benchmark run
`python benchmark_runner.py --script-id <script-id> --benchmark-run <int >= 1> --run-id <run-id>`. See the table
below for more information about
`--script-id` and `--run-id`.

| Flag              | Format / Pattern             | Meaning                                                                                                                                                       |
|-------------------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--script-id`     | `<query-type>-<service>`     | Identifies which query is being executed. `<query-type>` examples: `db-scan`, `bbox-filtering`. `<service>` examples: `blob-storage`, `postgis`.              |
| `--benchmark-run` | `int`                        | Identifier that tells which iteration of the benchmarking is currently running. This is to run the benchmarks on multiple container instances.                |
| `--run-id`        | `<current-date>-<random-id>` | Identifies a benchmark run. Shared across all queries in a single orchestrated run. Date format: `yyyy-mm-dd`; random ID: 6-character uppercase alphanumeric. |

