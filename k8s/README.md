# Kubernetes Deployment with Versioned Image Tags

## Overview

This directory contains Kubernetes configuration files for deploying the OSM pipeline as a job. The deployment uses versioned image tags (commit SHAs) instead of the `latest` tag to ensure deployments are deterministic and rollbacks are traceable.

## How It Works

### 1. Image Building and Tagging

The GitHub workflow `.github/workflows/publish-image-to-acr.yml` builds the Docker image and pushes it to Azure Container Registry with two tags:
- `latest` - for convenience during development
- `<commit-sha>` - for production deployments (e.g., `abc1234567890...`)

### 2. Deployment with Versioned Tags

The GitHub workflow `.github/workflows/deploy-to-k8s.yml` handles deployment:
1. Triggers automatically after a successful image build
2. Dynamically updates `job.yaml` to use the commit SHA tag
3. Applies the configuration to the Kubernetes cluster

This approach ensures:
- **Deterministic deployments**: Each deployment uses a specific, immutable image version
- **Easy rollbacks**: Previous versions can be deployed by their commit SHA
- **Audit trail**: Clear mapping between code version and deployed image

### 3. Manual Deployment

You can manually trigger a deployment with a specific image tag:

1. Go to Actions → "Deploy to Kubernetes" → "Run workflow"
2. Enter the image tag (commit SHA or other tag)
3. Run the workflow

## Configuration Files

- `job.yaml` - Kubernetes Job definition for the building conflation pipeline

## Example: Rolling Back to a Previous Version

To rollback to a previous version:

1. Find the commit SHA of the version you want to deploy
2. Manually trigger the "Deploy to Kubernetes" workflow
3. Enter the commit SHA as the image tag
4. The workflow will deploy that specific version

## Local Development

For local development, you can still use the `latest` tag. The versioned tag approach is primarily for production deployments where traceability and rollback capabilities are important.
