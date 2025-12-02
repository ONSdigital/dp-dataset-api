# dp-dataset-api SDK

## Overview

This SDK provides a client for interacting with the dp-dataset-api. It is intended to be consumed by services that require endpoints from the dp-dataset-api. It also provides healthcheck functionality, mocks and structs for easy integration and testing.

## Available client command

| Name | Description |
|------|-------------|
| [`Checker`](client.go) | Calls the health.Client's Checker method |
| [`Health`](client.go) | Returns the underlying Healthcheck Client for this API client |
| [`URL`](client.go) | returns the URL used by this client |
| [`GetDataset`](dataset.go) | Returns dataset level information for a given dataset id |
| [`GetDatasetByPath`](dataset.go) | Returns dataset level information for a given dataset path |
| [`GetDatasetEditions`](dataset.go) | Returns a list of dataset series that have unpublished versions or match the given state |
| [`CreateDataset`](dataset.go) | Creates a new dataset |
| [`GetEdition`](edition.go) | Retrieves a single edition document from a given datasetID and edition |
| [`GetEditions`](edition.go) | Returns a paginated list of editions for a dataset |
| [`GetVersion`](version.go) | Retrieves a specific version for an edition of a dataset |
| [`GetVersionV2`](version.go) | Same as `GetVersion` but expects `ErrorResponse` return error type |
| [`GetVersionDimensions`](version.go) | Returns a list of dimensions for a given version of a dataset |
| [`GetVersionDimensionOptions`](version.go) | Returns the options for a dimension |
| [`GetVersionMetadata`](version.go) | Returns the metadata for a given dataset id, edition and version |
| [`GetVersions`](version.go) | Returns a paginated list of versions for an edition |
| [`PutVersion`](version.go) | Updates a specific version for a dataset series |
| [`PutVersionState`](version.go) | Updates the state of a specific version for a dataset series |
| [`PostVersion`](version.go) | Creates a specific version for a dataset series |

## Instantiation

Example using `New`:

```go
package main

import "github.com/ONSdigital/dp-dataset-api/sdk"

func main() {
    client := sdk.New("http://localhost:22000")
}
```

Example using `NewWithHealthClient`:

```go
package main

import (
    "github.com/ONSdigital/dp-api-clients-go/v2/health"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

func main() {
    existingHealthClient := health.NewClient("existing-service-name", "http://localhost:8080")

    client := sdk.NewWithHealthClient(existingHealthClient)
}
```

## Example usage of client

This example demonstrates how the `GetDataset()` function could be used:

```go
package main

import (
    "context"

    "github.com/ONSdigital/dp-dataset-api/sdk"
)

func main() {
    client := sdk.New("http://localhost:22000")

    // Set headers if you want the request to be authenticated
    headers := sdk.Headers{
        ServiceToken: "example-service-token",
    }

    dataset, err := client.GetDataset(context.Background(), headers, "dataset-id")
    if err != nil {
        // Log the error and handle based on error message
    }

    // dataset object can be used as needed...
}
```

## Available functionality

### Errors

The dataset-api has multiple formats of which errors can be returned such as a plaintext string or a more informative [`ErrorResponse`](../models/responses.go). The SDK will return an `error` object which will contain the string of the error returned from the API. This could be used to string search for a specific status code or message for handling within an external service.

### Headers

The [`Headers`](client.go) struct allows the user to provide an Authorization header if required. This is shown in the [Example usage of client](#example-usage-of-client) section. The `"Bearer "` prefix will be added automatically.

This also provides options to add other headers such as `Collection-Id`, `X-Download-Service-Token` and `X-Florence-Token`.

### Mocks

To simplify testing, all functions provided by the client have been defined in the [`Clienter` interface](interface.go). This allows the user to use [auto-generated mocks](mocks/) within unit tests.

Example of how to define a mock clienter:

```go
import (
    "context"
    "testing"

    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
    "github.com/ONSdigital/dp-dataset-api/sdk/mocks"
)

func Test(t *testing.T) {
    mockClient := mocks.ClienterMock{
        GetDatasetFunc: func(ctx context.Context, headers sdk.Headers, collectionID, datasetID string) (models.Dataset, error) {
            // Setup mock behaviour here
            return models.Dataset{}, nil
        },
        // Other methods can be mocked if needed
    }
}
```
