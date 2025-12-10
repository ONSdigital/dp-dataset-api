# dp-dataset-api SDK

## Overview

This SDK provides a client for interacting with the dp-dataset-api. It is intended to be consumed by services that require endpoints from the dp-dataset-api. It also provides healthcheck functionality, mocks and structs for easy integration and testing.

## Available client methods

| Name | Description |
|------|-------------|
| [`Checker`](#checker) | Calls the health.Client's Checker method |
| [`Health`](#health) | Returns the underlying Healthcheck Client for this API client |
| [`URL`](#url) | returns the URL used by this client |
| [`GetDataset`](#getdataset) | Returns dataset level information for a given dataset id |
| [`GetDatasetCurrentAndNext`](#getdatasetcurrentandnext) | Returns dataset level information but contains both next and current documents |
| [`GetDatasetByPath`](#getdatasetbypath) | Returns dataset level information for a given dataset path |
| [`GetDatasetEditions`](#getdataseteditions) | Returns a list of dataset series that have unpublished versions or match the given state |
| [`GetDatasets`](#getdatasets) | Returns the list of datasets |
| [`GetDatasetsInBatches`](#getdatasetsinbatches) | Returns a list of datasets in concurrent batches and accumulates the results |
| [`CreateDataset`](#createdataset) | Creates a new dataset |
| [`GetEdition`](#getedition) | Retrieves a single edition document from a given datasetID and edition |
| [`GetEditions`](#geteditions) | Returns a paginated list of editions for a dataset |
| [`GetVersion`](#getversion) | Retrieves a specific version for an edition of a dataset |
| [`GetVersionV2`](#getversionv2) | Same as `GetVersion` but expects `ErrorResponse` return error type |
| [`GetVersionDimensions`](#getversiondimensions) | Returns a list of dimensions for a given version of a dataset |
| [`GetVersionDimensionOptions`](#getversiondimensionoptions) | Returns the options for a dimension |
| [`GetVersionMetadata`](#getversionmetadata) | Returns the metadata for a given dataset id, edition and version |
| [`GetVersionWithHeaders`](#getversionwithheaders) | gets a specific version for an edition from the dataset api and additional response headers (ETag) |
| [`GetVersions`](#getversions) | Returns a paginated list of versions for an edition |
| [`GetVersionsInBatches`](#getversionsinbatches) | Returns a list of dataset versions in concurrent batches and accumulates the results |
| [`PutDataset`](#putdataset) | Update the dataset |
| [`PutInstance`](#putinstance) | Updates an instance |
| [`PutMetadata`](#putmetadata) | Updates the dataset and the version metadata |
| [`PutVersion`](#putversion) | Updates a specific version for a dataset series |
| [`PutVersionState`](#putversionstate) | Updates the state of a specific version for a dataset series |
| [`PostVersion`](#postversion) | Creates a specific version for a dataset series |

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
        AccessToken: "example-service-token",
    }

    dataset, err := client.GetDataset(context.Background(), headers, "dataset-id")
    if err != nil {
        // Log the error and handle based on error message
    }

    // dataset object can be used as needed...
}
```

## Available functionality

### Checker

```go
import "github.com/ONSdigital/dp-healthcheck/healthcheck"

check := &healthcheck.CheckState{}
err := client.Checker(ctx, check)
```

### Health

```go
healthClient := client.Health()
```

### URL

```go
url := client.URL()
```

### GetDataset

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

dataset, err := client.GetDataset(ctx, headers, "dataset-id")
```

### GetDatasetByPath

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

dataset, err := client.GetDatasetByPath(ctx, headers, "/path/to/dataset")
```

### GetDatasetCurrentAndNext

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Required as this function returns the DatasetUpdate model which is specific to authorised users
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

dataset, err := client.GetDatasetCurrentAndNext(ctx, headers, "dataset-id")
```

### GetDatasetEditions

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

queryParams := &sdk.QueryParams{
    Limit:  10,
    Offset: 5,
}

datasetEditions, err := client.GetDatasetEditions(ctx, headers, queryParams)
```

### GetDatasets

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Required as this function returns thea list with items containing the DatasetUpdate model which is specific to authorised users
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

queryParams := &sdk.QueryParams{
    Limit:  10,
    Offset: 5,
}

datasets, err := client.GetDatasets(ctx, headers, queryParams)
```

### GetDatasetsInBatches

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Required as this function returns thea list with items containing the DatasetUpdate model which is specific to authorised users
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

datasets, err := client.GetDatasetsInBatches(ctx, headers, 10, 2)
```

### CreateDataset

```go
import (
    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

datasetToCreate := models.Dataset{
    ID:    "my-dataset-id",
    Title: "Title of this Dataset",
    // populate other required fields as required
}

createdDataset, err := client.CreateDataset(ctx, headers, datasetToCreate)
```

### GetEdition

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

edition, err := client.GetEdition(ctx, headers, "dataset-id", "edition-id")
```

### GetEditions

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    ServiceToken: "example-auth-token",
}

queryParams := &sdk.QueryParams{
    Limit:  10,
    Offset: 5,
}

editions, err := client.GetEditions(ctx, headers, "dataset-id", queryParams)
```

### GetVersion

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

version, err := client.GetVersion(ctx, headers, "dataset-id", "edition-id", "1")
```

### GetVersionWithHeaders

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

version, err := client.GetVersionWithHeaders(ctx, headers, "dataset-id", "edition-id", "1")
```

### GetVersionV2

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

version, err := client.GetVersionV2(ctx, headers, "dataset-id", "edition-id", "1")
```

### GetVersionDimensions

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

versionDimensions, err := client.GetVersionDimensions(ctx, headers, "dataset-id", "edition-id", "1")
```

### GetVersionDimensionOptions

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

queryParams := &sdk.QueryParams{
    Limit:  10,
    Offset: 5,
}

versionDimensionOptionsList, err := client.GetVersionDimensionOptions(ctx, headers, "dataset-id", "edition-id", "1", "dimension-id", queryParams)
```

### GetVersionMetadata

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

versionMetadata, err := client.GetVersionMetadata(ctx, headers, "dataset-id", "edition-id", "1")
```

### GetVersions

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    ServiceToken: "example-auth-token",
}

queryParams := &sdk.QueryParams{
    Limit:  10,
    Offset: 5,
}

versions, err := client.GetVersions(ctx, headers, "dataset-id", "edition-id", queryParams)
```

### GetVersionsInBatches

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

// Optional to get unpublished data
headers := sdk.Headers{
    ServiceToken: "example-auth-token",
}

versions, err := client.GetVersionsInBatches(ctx, headers, "dataset-id", "edition-id", 10, 2)
```

### PutDataset

```go
import (
    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

datasetToUpdate := models.Dataset{
    Type: models.Static.String(),
    Description: "this is a dataset",
    // populate other required fields as required
}

err := client.PutDataset(ctx, headers, "dataset-id", datasetToupdate)
```

### PutInstance

```go
import (
    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

instanceToUpdate := models.UpdateInstance{
    Type: models.CantabularFlexibleTable.String(),
    Edition: "time-series",
    // populate other required fields as required
}

returnedETag, err := client.PutInstance(ctx, headers, "instance-id", instanceToUpdate, "etag-value")
```

### PutMetadata

```go
import (
    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

metadataToUpdate := models.EditableMetadata{
    Title: "The dataset title",
    Description: "A description of the dataset",
    // populate other required fields as required
}

err := client.PutMetadata(ctx, headers, "dataset-id", "edition-id", "1", metadataToUpdate, "etag-value")
```

### PutVersion

```go
import (
    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

versionToUpdate := models.Version{
    Type: models.Static.String(),
    EditionTitle: "Updated Edition Title",
    // populate other required fields as required
}

updatedVersion, err := client.PutVersion(ctx, headers, "dataset-id", "edition-id", "1", versionToUpdate)
```

### PutVersionState

```go
import (
    "github.com/ONSdigital/dp-dataset-api/models"
    "github.com/ONSdigital/dp-dataset-api/sdk"
)

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

// Example to change to "approved" state
err := client.PutVersionState(ctx, headers, "dataset-id", "edition-id", "1", models.ApprovedState)
```

### PostVersion

```go
import "github.com/ONSdigital/dp-dataset-api/sdk"

headers := sdk.Headers{
    AccessToken: "example-auth-token",
}

versionToCreate := models.Version{
    Type:         models.Static.String(),
    Edition:      "edition-id",
    EditionTitle: "Edition Title",
    // populate other required fields as required
}

createdVersion, err := client.PostVersion(ctx, headers, "dataset-id", "edition-id", "1", versionToCreate)
```

## Additional Information

### Errors

The dataset-api has multiple formats of which errors can be returned such as a plaintext string or a more informative [`ErrorResponse`](../models/responses.go). The SDK will return an `error` object which will contain the string of the error returned from the API. This could be used to string search for a specific status code or message for handling within an external service.

### Headers

The [`Headers`](client.go) struct allows the user to provide an Authorization header if required. This is shown in the [Example usage of client](#example-usage-of-client) section. The `"Bearer "` prefix will be added automatically.

This also provides options to add other headers such as `Collection-Id` and `X-Download-Service-Token`.

### QueryParams

The [`QueryParams`](version.go) struct allows the user to provide query parameters if required. This is shown in the [GetVersions](#getversions) example. The options include `IDs`, `IsBasedOn`, `State`, `Limit` and `Offset`.

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
        GetDatasetFunc: func(ctx context.Context, headers sdk.Headers, datasetID string) (models.Dataset, error) {
            // Setup mock behaviour here
            return models.Dataset{}, nil
        },
        // Other methods can be mocked if needed
    }
}
```
