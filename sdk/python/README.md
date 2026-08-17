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

### Install as a local library in another service
Get the latest release tag from the [dp-dataset-api releases](https://github.com/ONSdigital/dp-dataset-api/releases) page and replace `<release-tag>` with the release tag in the command below.
```bash
pip install "git+https://github.com/ONSdigital/dp-dataset-api.git@<release-tag>#subdirectory=sdk/python"
```

## Public API

```python
from dis_dataset_api_sdk_python import DatasetApiClient, Dataset, Headers
from dis_dataset_api_sdk_python import (
    ApiError,
    AuthenticationError,
    NotFoundError,
    ValidationError,
)
```

## Quick Start

```python
from dis_dataset_api_sdk_python import DatasetApiClient

client = DatasetApiClient(base_url="https://api.example.com")
health = client.health()
print(health)
```

## Get Dataset

`get_dataset` returns a validated Pydantic `Dataset` model.

```python
from dis_dataset_api_sdk_python import DatasetApiClient

client = DatasetApiClient(base_url="https://api.example.com")
dataset = client.get_dataset("my-dataset-id")

print(dataset.id)
print(dataset.model_dump())
```

## Authentication and Headers

You can pass per-request headers using `Headers`.

```python
from dis_dataset_api_sdk_python import DatasetApiClient, Headers

client = DatasetApiClient(base_url="https://api.example.com")
dataset = client.get_dataset(
    "my-dataset-id",
    headers=Headers(
        Authorization="Bearer YOUR_TOKEN",
        CollectionID="my-collection-id",
    ),
)
```

You can also set default headers on a shared `requests.Session`.

```python
import requests
from dis_dataset_api_sdk_python import DatasetApiClient

session = requests.Session()
session.headers.update({"Authorization": "Bearer YOUR_TOKEN"})

client = DatasetApiClient(
    base_url="https://api.example.com",
    session=session,
)
```

## Error Handling

The client raises typed exceptions for common HTTP failures.

```python
from dis_dataset_api_sdk_python import DatasetApiClient
from dis_dataset_api_sdk_python import ApiError, AuthenticationError, NotFoundError

client = DatasetApiClient(base_url="https://api.example.com")

try:
    dataset = client.get_dataset("my-dataset-id")
except NotFoundError:
    print("Dataset does not exist")
except AuthenticationError:
    print("Authentication failed")
except ApiError as exc:
    print(f"API failed: status={exc.status_code} message={exc}")
```

## Run Unit Tests

```bash
make test
```

## Lint and Format

```bash
make lint
make format
```

