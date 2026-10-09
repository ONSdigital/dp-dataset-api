# dis-dataset-api-sdk-python

Python SDK for interacting with `dp-dataset-api`.

## Requirements

- Python `>=3.14`
- `requests`
- `pydantic`

## Install

### Install in this project with Makefile

```bash
make install
```

### Install from Git in another service
Replace `<release-tag>` with a tag from the [dp-dataset-api releases](https://github.com/ONSdigital/dp-dataset-api/releases) page.

```bash
pip install "git+https://github.com/ONSdigital/dp-dataset-api.git@<release-tag>#subdirectory=sdk/python"
```

## Public API

```python
from dis_dataset_api_sdk_python import (
    ApiError,
    AuthenticationError,
    DatasetApiClient,
    DatasetApiClientProtocol,
    DatasetsClientProtocol,
    EditionsClientProtocol,
    Headers,
    HttpHeaders,
    NotFoundError,
    ValidationError,
    VersionsClientProtocol,
    create_client,
)
```

## SDK Methods Reference

| Method | HTTP Endpoint | Description |
|--------|---------------|-------------|
| `client.health()` | `GET /health` | Check API health status |
| `client.datasets.get_dataset()` | `GET /datasets/{dataset_id}` | Get a specific dataset |
| `client.datasets.get_dataset_by_path()` | `GET /{path}` | Get dataset by custom path |
| `client.datasets.get_dataset_editions()` | `GET /dataset-editions` | Get all dataset editions with optional filters |
| `client.datasets.get_datasets()` | `GET /datasets` | Get all datasets with optional filters |
| `client.editions.get_edition()` | `GET /datasets/{dataset_id}/editions/{edition_id}` | Get a specific edition |
| `client.editions.get_editions()` | `GET /datasets/{dataset_id}/editions` | Get all editions for a dataset |
| `client.versions.get_version()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}` | Get a specific version |
| `client.versions.get_version_metadata()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}/metadata` | Get version metadata |
| `client.versions.get_version_dimensions()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}/dimensions` | Get version dimensions |
| `client.versions.get_version_dimension_options()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions/{version_id}/dimensions/{dimension_id}/options` | Get dimension options for a version |
| `client.versions.get_versions()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions` | Get all versions for an edition |
| `client.versions.get_versions_in_batches()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions` | Fetch all versions in batches with concurrent requests |
| `client.versions.get_versions_in_batches_with_query_params()` | `GET /datasets/{dataset_id}/editions/{edition_id}/versions` | Fetch versions in batches with pagination and concurrent requests |

## Quick Start

```python
from dis_dataset_api_sdk_python import create_client

client = create_client(base_url="https://api.example.com")
health = client.health()

print(health)
```

## Working with datasets

Use the `datasets` resource for dataset lookups.

```python
from dis_dataset_api_sdk_python import create_client

client = create_client(base_url="http://localhost:22000")
dataset = client.datasets.get_dataset("my-dataset-id")

print(dataset.id)
print(dataset.title)
print(dataset.model_dump())
```

The dataset resource also exposes:

- `client.datasets.get_dataset_by_path(...)`
- `client.datasets.get_dataset_editions(...)`
- `client.datasets.get_datasets(...)`

## Working with editions

Use the `editions` resource for edition lookups.

```python
from dis_dataset_api_sdk_python import create_client

client = create_client(base_url="http://localhost:22000")
edition = client.editions.get_edition("my-dataset-id", "2024")

print(edition.id)
print(edition.edition)
```

The editions resource also exposes:

- `client.editions.get_editions(...)`

## Working with versions

Use the `versions` resource for version lookups.

```python
from dis_dataset_api_sdk_python import create_client

client = create_client(base_url="http://localhost:22000")
version = client.versions.get_version("my-dataset-id", "2024", "1")

print(version.id)
print(version.version)
```

The versions resource also exposes:

- `client.versions.get_version_metadata(...)`
- `client.versions.get_version_dimensions(...)`
- `client.versions.get_version_dimension_options(...)`
- `client.versions.get_versions(...)`
- `client.versions.get_versions_in_batches(...)`
- `client.versions.get_versions_in_batches_with_query_params(...)`

## Headers and authentication

You can pass per-request headers using `HttpHeaders`.

```python
from dis_dataset_api_sdk_python import HttpHeaders, create_client

client = create_client(base_url="http://localhost:22000")

dataset = client.datasets.get_dataset(
    "my-dataset-id",
    headers=HttpHeaders(
        authorization="Bearer YOUR_TOKEN",
        collection_id="my-collection-id",
        if_match="etag-1",
        download_service_token="download-token",
    ),
)
```

`HttpHeaders` omits `None` values automatically before sending the request.

This SDK is designed to be used with a shared `requests.Session`. Pass one into `create_client()` so your requests reuse connections and can share default headers.

```python
import requests

from dis_dataset_api_sdk_python import create_client

session = requests.Session()
session.headers.update({"Authorization": "Bearer YOUR_TOKEN"})

client = create_client(
    base_url="http://localhost:22000",
    session=session,
)
```

## Error handling

The client raises typed exceptions for common HTTP failures.

```python
from dis_dataset_api_sdk_python import (
    ApiError,
    AuthenticationError,
    NotFoundError,
    ValidationError,
    create_client,
)

client = create_client(base_url="http://localhost:22000")

try:
    dataset = client.datasets.get_dataset("my-dataset-id")
except NotFoundError:
    print("Dataset does not exist")
except AuthenticationError:
    print("Authentication failed")
except ValidationError as exc:
    print(f"Validation failed: {exc}")
except ApiError as exc:
    print(f"API failed: status={exc.status_code} message={exc}")
```

Some list-style methods return `(result, error)` instead of raising for parameter validation errors. For example:

```python
datasets, error = client.datasets.get_datasets(headers=HttpHeaders())
if error is not None:
    print(error)
```

## Development commands

```bash
make install      # Install dependencies (main only)
make install-dev  # Install dependencies (including dev)
make format       # Format the code
make lint         # Run linting checks
make mypy         # Run type checking
make test         # Run tests with coverage
make audit        # Check for vulnerabilities
```
