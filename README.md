# doppa: A Framework for comparing traditional and cloud-native geospatial queries

[![Build and Push Query- and Orchestration-containers to Azure Container Registry](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml/badge.svg)](https://github.com/kartAI/doppa-data/actions/workflows/push-containers-to-acr.yml) [![Run Benchmarks](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml/badge.svg?event=schedule)](https://github.com/kartAI/doppa-data/actions/workflows/run-benchmarks.yml)

## Setup

Add the following `.env` file

```dotenv
AZURE_BLOB_STORAGE_CONNECTION_STRING=<azure-blob-storage-connection-string>
ACR_LOGIN_SERVER=<azure-container-registry-login-server>
ACR_USERNAME=<azure-container-registry-username>
ACR_PASSWORD=<azure-container-registry-password>
```