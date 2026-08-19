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
    Dataset,
    DatasetApiClientProtocol,
    HttpHeaders,
    Headers,
    NotFoundError,
    RequestingClient,
    ValidationError,
    create_client,
)
```

## Quick Start

```python
from dis_dataset_api_sdk_python import create_client

client = create_client(base_url="https://api.example.com")
health = client.health()

print(health)
```

## Get Dataset

client = create_client(base_url="https://api.example.com", timeout=5.0)
```

## Get dataset

The `datasets` resource provides access to dataset operations. Use `client.datasets.get_dataset()` to retrieve a validated Pydantic `Dataset` model.

```python
from dis_dataset_api_sdk_python import create_client

client = create_client(base_url="https://api.example.com")
dataset = client.datasets.get_dataset("my-dataset-id")

print(dataset.id)
print(dataset.title)
print(dataset.model_dump())
```

## Headers and authentication

You can pass per-request headers using `HttpHeaders`.

```python
from dis_dataset_api_sdk_python import HttpHeaders, create_client

client = create_client(base_url="https://api.example.com")

dataset = client.datasets.get_dataset(
    "my-dataset-id",
    headers=HttpHeaders(
        Authorization="Bearer YOUR_TOKEN",
        CollectionID="my-collection-id",
    ),
)
```

`HttpHeaders` omits `None` values automatically before sending the request.

You can also set default headers on a shared `requests.Session`.

```python
import requests

from dis_dataset_api_sdk_python import DatasetApiClient

session = requests.Session()
session.headers.update({"Authorization": "Bearer YOUR_TOKEN"})

client = create_client(
    base_url="https://api.example.com",
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

client = create_client(base_url="https://api.example.com")

try:
    dataset = client.get_dataset("my-dataset-id")
except NotFoundError:
    print("Dataset does not exist")
except AuthenticationError:
    print("Authentication failed")
except ValidationError as exc:
    print(f"Validation failed: {exc}")
except ApiError as exc:
    print(f"API failed: status={exc.status_code} message={exc}")
```

## Development commands

```bash
make test
make typecheck
make lint
make format
make audit
```
