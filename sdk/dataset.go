package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dataset-api/models"
)

// List represents an object containing a list of datasets
type List struct {
	Items      []models.DatasetUpdate `json:"items"`
	Count      int                    `json:"count"`
	Offset     int                    `json:"offset"`
	Limit      int                    `json:"limit"`
	TotalCount int                    `json:"total_count"`
}

// DatasetsBatchProcessor is the type corresponding to a batch processing function for a dataset List.
type DatasetsBatchProcessor func(List) (abort bool, err error)

// ErrInvalidDatasetAPIResponse is returned when the dataset api does not respond
// with a valid status
type ErrInvalidDatasetAPIResponse struct {
	actualCode int
	uri        string
	body       string
}

// Error should be called by the user to print out the stringified version of the error
func (e ErrInvalidDatasetAPIResponse) Error() string {
	return fmt.Sprintf("invalid response: %d from dataset api: %s, body: %s",
		e.actualCode,
		e.uri,
		e.body,
	)
}

// Code returns the status code received from dataset api if an error is returned
func (e ErrInvalidDatasetAPIResponse) Code() int {
	return e.actualCode
}

var _ error = ErrInvalidDatasetAPIResponse{}

// DatasetAPIResponse creates an error response, optionally adding body to e when status is 404
func DatasetAPIResponse(resp *http.Response, uri string) (e *ErrInvalidDatasetAPIResponse) {
	e = &ErrInvalidDatasetAPIResponse{
		actualCode: resp.StatusCode,
		uri:        uri,
	}
	if resp.StatusCode == http.StatusNotFound {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			e.body = "Client failed to read DatasetAPI body"
			return
		}
		defer closeResponseBody(context.TODO(), resp)

		e.body = string(b)
	}
	return
}

// Get returns dataset level information for a given dataset id
func (c *Client) GetDataset(ctx context.Context, headers Headers, datasetID string) (dataset models.Dataset, err error) {
	dataset = models.Dataset{}

	fmt.Println("GETTING DATASET")

	// Build URI
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID)
	if err != nil {
		return dataset, err
	}
	// Make request
	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return dataset, err
	}
	defer closeResponseBody(ctx, resp)

	// If response got errors
	if resp.StatusCode != http.StatusOK {
		err = unmarshalResponseBodyExpectingStringError(resp, &dataset)
		return dataset, err
	}

	// Read the response body (only if status is OK)
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return dataset, err
	}

	var bodyMap map[string]interface{}
	if err := json.Unmarshal(b, &bodyMap); err != nil {
		return dataset, err
	}

	// If authenticated, try to extract "next" field from the JSON body
	if next, ok := bodyMap["next"]; ok && headers.AccessToken != "" {
		b, err = json.Marshal(next)
		if err != nil {
			return dataset, err
		}
	}

	resp.Body = io.NopCloser(bytes.NewReader(b))
	err = json.Unmarshal(b, &dataset)
	return dataset, err
}

// GetDatasetCurrentAndNext returns dataset level information but contains both next and current documents
func (c *Client) GetDatasetCurrentAndNext(ctx context.Context, headers Headers, datasetID string) (dataset models.DatasetUpdate, err error) {
	dataset = models.DatasetUpdate{}

	// Build URI
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID)
	if err != nil {
		return dataset, err
	}

	// Make request
	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return dataset, err
	}
	defer closeResponseBody(ctx, resp)

	if resp.StatusCode != http.StatusOK {
		err = unmarshalResponseBodyExpectingStringError(resp, &dataset)
		return dataset, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return dataset, err
	}

	if err := json.Unmarshal(b, &dataset); err != nil {
		return dataset, err
	}

	return dataset, nil
}

// GetDatasetByPath returns dataset level information for a given dataset path
func (c *Client) GetDatasetByPath(ctx context.Context, headers Headers, path string) (dataset models.Dataset, err error) {
	dataset = models.Dataset{}
	// Build uri
	uri := &url.URL{}
	trimmedPath := strings.Trim(path, "/")
	uri.Path, err = url.JoinPath(c.hcCli.URL, trimmedPath)
	if err != nil {
		return dataset, err
	}

	// Make request
	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return dataset, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &dataset)

	return dataset, err
}

// DatasetEditionsList represents an object containing a list of paginated dataset editions. This struct is based
// on the `pagination.page` struct which is returned when we call the `api.getDatasetEditions` endpoint
type DatasetEditionsList struct {
	Items      []models.DatasetEdition `json:"items"`
	Count      int                     `json:"count"`
	Offset     int                     `json:"offset"`
	Limit      int                     `json:"limit"`
	TotalCount int                     `json:"total_count"`
}

// GetDatasetEditions returns a list of dataset series that have unpublished versions or match the given state
func (c *Client) GetDatasetEditions(ctx context.Context, headers Headers, queryParams *QueryParams) (datasetEditionsList DatasetEditionsList, err error) {
	datasetEditionsList = DatasetEditionsList{}

	// Build URI
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "dataset-editions")
	if err != nil {
		return datasetEditionsList, err
	}

	// Add query parameters to request if valid
	if queryParams != nil {
		if err := queryParams.Validate(); err != nil {
			return datasetEditionsList, err
		}

		// Add query parameters
		query := url.Values{}
		query.Add("limit", strconv.Itoa(queryParams.Limit))
		query.Add("offset", strconv.Itoa(queryParams.Offset))
		if queryParams.State != "" {
			query.Add("state", queryParams.State)
		}
		uri.RawQuery = query.Encode()
	}

	// Make request
	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return datasetEditionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &datasetEditionsList)

	return datasetEditionsList, err
}

// GetDatasetsInBatches retrieves a list of datasets in concurrent batches and accumulates the results
func (c *Client) GetDatasetsInBatches(ctx context.Context, headers Headers, batchSize, maxWorkers int) (datasets List, err error) {
	// Function to aggregate items.
	// For the first received batch, as we have the total count information, will initialise the final structure of items with a fixed size equal to TotalCount.
	// This serves two purposes:
	//   - We can guarantee, even with concurrent calls, that values are returned in the same order that the API defines, by offsetting the index.
	//   - We do a single memory allocation for the final array, making the code more memory efficient.
	var processBatch DatasetsBatchProcessor = func(b List) (abort bool, err error) {
		if len(datasets.Items) == 0 { // first batch response being handled
			datasets.TotalCount = b.TotalCount
			datasets.Items = make([]models.DatasetUpdate, b.TotalCount)
			datasets.Count = b.TotalCount
		}
		for i := 0; i < len(b.Items); i++ {
			datasets.Items[i+b.Offset] = b.Items[i]
		}
		return false, nil
	}

	// call dataset API GetOptions in batches and aggregate the responses
	if err := c.GetDatasetsBatchProcess(ctx, headers, processBatch, batchSize, maxWorkers); err != nil {
		return List{}, err
	}

	return datasets, nil
}

// GetDatasetsBatchProcess gets the datasets from the dataset API in batches, calling the provided function for each batch.
func (c *Client) GetDatasetsBatchProcess(ctx context.Context, headers Headers, processBatch DatasetsBatchProcessor, batchSize, maxWorkers int) error {
	// for each batch, obtain the dimensions starting at the provided offset, with a batch size limit,
	// or the subste of IDs according to the provided offset, if a list of optionIDs was provided
	batchGetter := func(offset int) (interface{}, int, string, error) {
		b, err := c.GetDatasets(ctx, headers, &QueryParams{Offset: offset, Limit: batchSize})
		return b, b.TotalCount, "", err
	}

	// cast and process the batch according to the provided method
	batchProcessor := func(b interface{}, batchETag string) (abort bool, err error) {
		v, ok := b.(List)
		if !ok {
			return true, errors.New("wrong type")
		}
		return processBatch(v)
	}

	return ProcessInConcurrentBatches(batchGetter, batchProcessor, batchSize, maxWorkers)
}

// GetDatasets returns the list of datasets
func (c *Client) GetDatasets(ctx context.Context, headers Headers, q *QueryParams) (datasets List, err error) {
	// Build URI
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets")
	if err != nil {
		return List{}, err
	}

	if q != nil {
		if err := q.Validate(); err != nil {
			return List{}, err
		}

		// Add query parameters
		query := url.Values{}
		query.Add("offset", strconv.Itoa(q.Offset))
		query.Add("limit", strconv.Itoa(q.Limit))
		if q.IsBasedOn != "" {
			query.Add("is_based_on", q.IsBasedOn)
		}
		uri.RawQuery = query.Encode()
	}

	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return List{}, err
	}
	defer closeResponseBody(ctx, resp)

	if resp.StatusCode != http.StatusOK {
		err = DatasetAPIResponse(resp, uri.RequestURI())
		return List{}, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return List{}, err
	}

	if err := json.Unmarshal(b, &datasets); err != nil {
		return List{}, err
	}

	return datasets, nil
}

// PutDataset update the dataset
func (c *Client) PutDataset(ctx context.Context, headers Headers, datasetID string, d models.Dataset) error {
	var err error
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(d)
	if err != nil {
		return errors.New("error while attempting to marshall dataset")
	}

	resp, err := c.doAuthenticatedPutRequest(ctx, headers, uri, payload)
	if err != nil {
		return err
	}
	defer closeResponseBody(ctx, resp)

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusMultipleChoices {
		responseBody, err := getStringResponseBody(resp)
		if err != nil {
			return fmt.Errorf("did not receive success response. received status %d", resp.StatusCode)
		}
		return fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, *responseBody)
	}
	return nil
}

// CreateDataset creates a new dataset by posting to the POST /datasets endpoint
func (c *Client) CreateDataset(ctx context.Context, headers Headers, dataset models.Dataset) (models.DatasetUpdate, error) {
	var datasetUpdate models.DatasetUpdate

	// Build URI
	uri := &url.URL{}
	var err error
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets")
	if err != nil {
		return datasetUpdate, err
	}

	// Marshal dataset to JSON
	payload, err := json.Marshal(dataset)
	if err != nil {
		return datasetUpdate, err
	}

	// Make request
	resp, err := c.doAuthenticatedPostRequest(ctx, headers, uri, payload)
	if err != nil {
		return datasetUpdate, err
	}

	defer closeResponseBody(ctx, resp)

	// If response got errors
	if resp.StatusCode != http.StatusCreated {
		err = unmarshalResponseBodyExpectingStringError(resp, &datasetUpdate)
		return datasetUpdate, err
	}

	// Read and unmarshal the response body for successful creation
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return datasetUpdate, err
	}

	err = json.Unmarshal(b, &datasetUpdate)
	return datasetUpdate, err
}
