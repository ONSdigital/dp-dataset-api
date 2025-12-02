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

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dpNetRequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/ONSdigital/log.go/v2/log"
)

const (
	service = "dp-dataset-api"
)

type Client struct {
	hcCli *health.Client
}

// Contains the headers to be added to any request
type Headers struct {
	CollectionID         string
	DownloadServiceToken string
	ServiceToken         string
	UserAccessToken      string
}

// Adds headers to the input request
func (h *Headers) add(request *http.Request) {
	if h.CollectionID != "" {
		request.Header.Add(dpNetRequest.CollectionIDHeaderKey, h.CollectionID)
	}
	dpNetRequest.AddDownloadServiceTokenHeader(request, h.DownloadServiceToken)
	dpNetRequest.AddFlorenceHeader(request, h.UserAccessToken)
	dpNetRequest.AddServiceTokenHeader(request, h.ServiceToken)
}

// Checker calls the health.Client's Checker method
func (c *Client) Checker(ctx context.Context, check *healthcheck.CheckState) error {
	return c.hcCli.Checker(ctx, check)
}

// Creates new request object, executes a get request using the input `headers` and `uri` and returns the response
func (c *Client) doAuthenticatedGetRequest(ctx context.Context, headers Headers, uri *url.URL) (resp *http.Response, err error) {
	resp = &http.Response{}
	req, err := http.NewRequest(http.MethodGet, uri.RequestURI(), http.NoBody)
	if err != nil {
		return resp, err
	}

	// Add auth headers to the request
	headers.add(req)

	return c.hcCli.Client.Do(ctx, req)
}

// Creates new request object, executes a put request using the input `headers`, `uri`, and payload, and returns the response
func (c *Client) doAuthenticatedPutRequest(ctx context.Context, headers Headers, uri *url.URL, payload []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPut, uri.RequestURI(), bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	headers.add(req)
	return c.hcCli.Client.Do(ctx, req)
}

// Creates new request object, executes a post request using the input `headers`, `uri`, and payload, and returns the response
func (c *Client) doAuthenticatedPostRequest(ctx context.Context, headers Headers, uri *url.URL, payload []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, uri.RequestURI(), bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	headers.add(req)
	return c.hcCli.Client.Do(ctx, req)
}

// Health returns the underlying Healthcheck Client for this API client
func (c *Client) Health() *health.Client {
	return c.hcCli
}

// URL returns the URL used by this client
func (c *Client) URL() string {
	return c.hcCli.URL
}

// New creates a new instance of Client for the service
func New(datasetAPIUrl string) *Client {
	return &Client{
		hcCli: health.NewClient(service, datasetAPIUrl),
	}
}

// NewWithHealthClient creates a new instance of service API Client, reusing the URL and Clienter
// from the provided healthcheck client
func NewWithHealthClient(hcCli *health.Client) *Client {
	return &Client{
		hcCli: health.NewClientWithClienter(service, hcCli.URL, hcCli.Client),
	}
}

// closeResponseBody closes the response body and logs an error if unsuccessful
func closeResponseBody(ctx context.Context, resp *http.Response) {
	if resp.Body != nil {
		if err := resp.Body.Close(); err != nil {
			log.Error(ctx, "error closing http response body", err)
		}
	}
}

// Takes the input http response and unmarshals the body to the input target
func unmarshalResponseBodyExpectingStringError(response *http.Response, target interface{}) (err error) {
	// Read the entire response body first, regardless of status code.
	b, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return fmt.Errorf("failed to read response body: %w", readErr)
	}
	// Restore the body for subsequent reads if needed
	response.Body = io.NopCloser(bytes.NewReader(b))

	if response.StatusCode != http.StatusOK {
		var errString string
		// Attempt to unmarshal as a JSON string first
		if jsonErr := json.Unmarshal(b, &errString); jsonErr == nil {
			// Successfully unmarshaled as a JSON string
			return errors.New(errString)
		}

		// If it's not a JSON string, treat it as a plain text string
		plainTextErr := strings.TrimSpace(string(b))

		if plainTextErr == "" {
			return fmt.Errorf("API returned status %d with an empty error message", response.StatusCode)
		}
		return errors.New(plainTextErr)
	}

	// If status is OK, unmarshal the body into the target.
	if len(b) == 0 {
		return errors.New("received 200 OK but response body is empty")
	}

	return json.Unmarshal(b, &target)
}

func unmarshalResponseBodyExpectingErrorResponse(response *http.Response, target interface{}) (err error) {
	if response.StatusCode != http.StatusOK {
		var errResponse models.ErrorResponse
		errResponseReadErr := json.NewDecoder(response.Body).Decode(&errResponse)
		if errResponseReadErr != nil {
			return errors.New("Client failed to read DatasetAPI body")
		}
		return errResponse.Errors[0]
	}

	b, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &target)
}

func unmarshalResponseBodyExpectingErrorResponseV2(response *http.Response, target interface{}) (err error) {
	if response.StatusCode != http.StatusOK {
		var errResponse models.ErrorResponse
		errResponseReadErr := json.NewDecoder(response.Body).Decode(&errResponse)
		if errResponseReadErr != nil {
			return errors.New("Client failed to read DatasetAPI body")
		}
		if response.StatusCode == http.StatusNotFound {
			errResponse.Errors[0].Cause = apierrors.ErrVersionNotFound
			errResponse.Errors[0].Code = strconv.Itoa(http.StatusNotFound)
			return errResponse.Errors[0]
		}
	}

	b, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &target)
}

// unmarshalErrorResponse unmarshals the response body into an ErrorResponse
func unmarshalErrorResponse(body io.ReadCloser) (*models.ErrorResponse, error) {
	if body == nil {
		return nil, errors.New("response body is nil")
	}

	var errorResponse models.ErrorResponse

	err := json.NewDecoder(body).Decode(&errorResponse)
	if err != nil {
		return nil, err
	}

	return &errorResponse, nil
}

func getStringResponseBody(resp *http.Response) (*string, error) {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("failed to read response body")
	}

	bodyString := string(bodyBytes)

	return &bodyString, nil
}
