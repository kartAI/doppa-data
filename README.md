# doppa: A Framework for Comparing Traditional & CNG Queries

[![Build and Push Query- and Orchestration-containers to Azure Container Registry](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml/badge.svg)](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml) [![Publish APIs](https://github.com/kartAI/doppa-data/actions/workflows/publish-api.yml/badge.svg)](https://github.com/kartAI/doppa-data/actions/workflows/publish-api.yml) [![Run Benchmarks](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml/badge.svg?event=schedule)](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml)

## Table of contents
- [Setup](#setup)
  - [Azure Resources](#azure-resources)
    - [Resource group](#resource-group)
    - [Blob storage](#blob-storage)
    - [User-Assigned Managed Identity (UAMI)](#user-assigned-managed-identity-uami)
    - [Container registry](#container-registry)
    - [PostgreSQL database](#postgresql-database)
    - [Web app for containers](#web-app-for-containers)
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
datasets are stored here. Create a storage account named `doppablobstorage`. There is no need to create the containers
as these are created during runtime. Each container is created with the `Container` access level. If you wish to make
this stricter make the following changes in the `ensure_container` function in [
`BlobStorageService`](./src/infra/infrastructure/services/blob_storage_service.py).

```powershell
self.__blob_storage_context.create_container(container_name.value, public_access = PublicAccess.CONTAINER)    # From this
self.__blob_storage_context.create_container(container_name.value, public_access = PublicAccess.BLOB)         # To this
```

#### User-Assigned Managed Identity (UAMI)

To provide the correct access to Azure resources when running the script from GitHub Actions a UAMI have to be
configured. The Actions will sign in to Azure and executes the scripts using the UAMI. Create a UAMI named
`github-actions-ci` and navigate to the *Federated credentials* setting. Create two federated credentials with the
following setup:

<div style="display:flex; justify-content:center; align-items:flex-start; gap:20px;">
  <img src=".github/docs/img/github-ci-fc-main.png" width="45%" />
  <img src=".github/docs/img/github-ci-fc-pr.png" width="45%" />
</div>

Change the fields according to your setup.

The next step is to give the UAMI a `Contributor` in the resource group. Navigate to the *Azure role assignments*
setting and press *Add role assignment*. Select the scope `Resource group` and then the resource group `doppa`. Pick the
role `Contributor` and press *Save*.

#### Container registry

Create a container registry named `doppaacr`. The Docker images will be saved here. To ensure that the Actions are able
to pull the images give the UAMI created in the last step a `AcrPull` role. In the `doppaacr` resource navigate to
*Access control (IAM)* and press *Add* > *Add role assignment*. Select the role `AcrPull` and continue. On the next
screen select *Managed identity* under *Assign access to*, and select the `doppa-github-ci` UAMI under *Members*.
Navigate to the last step and press *create*.

#### PostgreSQL database

Create
an [Azure database for PostgreSQL](https://portal.azure.com/#view/Microsoft_Azure_Marketplace/GalleryItemDetailsBladeNopdl/id/Microsoft.PostgreSQLServer/selectionMode~/false/resourceGroupId//resourceGroupLocation//dontDiscardJourney~/false/selectedMenuId/home/launchingContext~/%7B%22galleryItemId%22%3A%22Microsoft.PostgreSQLServer%22%2C%22source%22%3A%5B%22GalleryFeaturedMenuItemPart%22%2C%22VirtualizedTileDetails%22%5D%2C%22menuItemId%22%3A%22home%22%2C%22subMenuItemId%22%3A%22Search%20results%22%2C%22telemetryId%22%3A%22a28a8a60-8a59-43fd-8def-fc6cba1ca11f%22%7D/searchTelemetryId/0a3db32e-00e8-4ba8-b921-45bc0a7a5a28)
with the following configuration:

Under *Basics*:

- Server name: `doppa-data`
- Region: `Norway East`
- Workload type: `Production`
- Compute + Storage: Disable `Geo-Redundancy` and leave everything else as is
- Zonal resiliency: `Disabled`
- Authentication method: `PostgreSQL authentication only`

Under *Networking*:

- Firewall rules: Check the box *Allow public access from any Azure service within Azure to this server*.
- Add current IP address to Firewall rules

Navigate to *Review and create* and create the resource.

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
- Identity: `doppa-github-ci`
- Image: `<select the image that matches with the name>`
- Tag: `latest`
- Startup command `uvicorn src.presentation.endpoints.<API server script>:app --host 0.0.0.0 --port 8000`

Navigate to *Review + create* and create the resource. Repeat this process for each name in the list.

### GitHub Actions

In your repository navigate to *Secrets and variables* under *Settings*. Add the following **secrets**:

- `ACR_NAME`
- `ACR_PASSWORD`
- `ACR_USERNAME`
- `AZURE_BLOB_STORAGE_CONNECTION_STRING`
- `POSTGRES_USERNAME`
- `POSTGRES_PASSWORD`

and add the following **variables**:

- `ACR_LOGIN_SERVER`
- `AZURE_BLOB_STORAGE_BENCHMARK_CONTAINER`
- `AZURE_BLOB_STORAGE_METADATA_CONTAINER`
- `AZURE_CLIENT_ID`
- `AZURE_RESOURCE_GROUP`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_TENANT_ID`

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
python -m venv venv             # Create virtual enviornment
venv/Scripts/activate           # Activate venv
pip freeze > requirements.txt   # Install dependencies
```

Add the following `.env` file to the project root directory. Swap out the values enclosed by `<>` with the actual
secrets. The containers `dev-benchmakrs` and `dev-metadata` ensure that results from the test runs do not disrupt
results from actual runs.

```dotenv
AZURE_BLOB_STORAGE_CONNECTION_STRING=<azure-blob-storage-connection-string>
AZURE_BLOB_STORAGE_BENCHMARK_CONTAINER=dev-benchmarks
AZURE_BLOB_STORAGE_METADATA_CONTAINER=dev-metadata

ACR_LOGIN_SERVER=<azure-container-registry-login-server>
ACR_USERNAME=<azure-container-registry-username>
ACR_PASSWORD=<azure-container-registry-password>

POSTGRES_USERNAME=<postgres-username>
POSTGRES_PASSWORD=<postgres-password>
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

