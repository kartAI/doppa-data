# doppa: A Framework for Comparing Traditional & Cloud-Native Geospatial Queries

[![Build and Push Query- and Orchestration-containers to Azure Container Registry](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml/badge.svg)](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml) [![Run Benchmarks](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml/badge.svg?event=schedule)](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml)

## Setup

Create a virtual environment and install the dependencies in the [requirements-file](./requirements.txt).

Add the following `.env` file in the root

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

> [!NOTE]
> Ensure that the needed Azure Resources have been configured

To run the entire script simply run `python main.py` or `python -m main` and to run a single benchmark run
`python benchmark_runner.py --script-id <script-id> --benchmark-iteration <int >= 1> --run-id <run-id>`. See the table
below for more information about
`--script-id` and `--run-id`.

| Flag                    | Format / Pattern             | Meaning                                                                                                                                                       |
|-------------------------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--script-id`           | `<query-type>-<service>`     | Identifies which query is being executed. `<query-type>` examples: `db-scan`, `bbox-filtering`. `<service>` examples: `blob-storage`, `postgis`.              |
| `--benchmark_iteration` | `int`                        | Identifier that tells which iteration of the benchmarking is currently running. This is to run the benchmarks on multiple container instances.                |
| `--run-id`              | `<current-date>-<random-id>` | Identifies a benchmark run. Shared across all queries in a single orchestrated run. Date format: `yyyy-mm-dd`; random ID: 6-character uppercase alphanumeric. |

