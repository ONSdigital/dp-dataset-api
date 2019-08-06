package gremgo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

const (
	StatusSuccess                  = 200
	StatusNoContent                = 204
	StatusPartialContent           = 206
	StatusUnauthorized             = 401
	StatusAuthenticate             = 407
	StatusMalformedRequest         = 498
	StatusInvalidRequestArguments  = 499
	StatusServerError              = 500
	StatusScriptEvaluationError    = 597
	StatusServerTimeout            = 598
	StatusServerSerializationError = 599
)

// Status struct is used to hold properties returned from requests to the gremlin server
type Status struct {
	Message    string                 `json:"message"`
	Code       int                    `json:"code"`
	Attributes map[string]interface{} `json:"attributes"`
}

// Result struct is used to hold properties returned for results from requests to the gremlin server
type Result struct {
	// Query Response Data
	Data json.RawMessage        `json:"data"`
	Meta map[string]interface{} `json:"meta"`
}

// Response structs holds the entire response from requests to the gremlin server
type Response struct {
	RequestID string `json:"requestId"`
	Status    Status `json:"status"`
	Result    Result `json:"result"`
}

// ToString returns a string representation of the Response struct
func (r Response) ToString() string {
	return fmt.Sprintf("Response \nRequestID: %v, \nStatus: {%#v}, \nResult: {%#v}\n", r.RequestID, r.Status, r.Result)
}

func (c *Client) saveWorkerCtx(ctx context.Context, msgChan chan []byte, errs chan error) {
	for {
		select {
		case msg := <-msgChan:
			if err := c.handleResponse(msg); err != nil {
				errs <- errors.Wrapf(err, "saveWorkerCtx: handleResponse error")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) handleResponse(msg []byte) (err error) {
	var resp Response
	resp, err = marshalResponse(msg)
	if resp.Status.Code == StatusAuthenticate { //Server request authentication
		return c.authenticate(resp.RequestID)
	}
	c.saveResponse(resp, err)
	return
}

// marshalResponse creates a response struct for every incoming response for further manipulation
func marshalResponse(msg []byte) (resp Response, err error) {
	err = json.Unmarshal(msg, &resp)
	if err != nil {
		return
	}

	err = resp.detectError()
	return
}

// saveResponse makes the response (and its err) available for retrieval by the requester.
// Mutexes are used for thread safety.
func (c *Client) saveResponse(resp Response, err error) {
	c.Lock()
	defer c.Unlock()
	var newdata []interface{}
	existingData, ok := c.results.Load(resp.RequestID) // Retrieve old data container (for requests with multiple responses)
	if ok {
		newdata = append(existingData.([]interface{}), resp) // Create new data container with new data
		existingData = nil
	} else {
		newdata = append(newdata, resp)
	}
	c.results.Store(resp.RequestID, newdata) // Add new data to buffer for future retrieval
	respNotifier, _ := c.responseNotifier.LoadOrStore(resp.RequestID, make(chan error, 1))
	// err is from marshalResponse (json.Unmarshal), but is ignored when Code==statusPartialContent
	if resp.Status.Code == StatusPartialContent {
		if chunkNotifier, ok := c.chunkNotifier.Load(resp.RequestID); ok {
			chunkNotifier.(chan bool) <- true
		}
	} else {
		respNotifier.(chan error) <- err
	}
}

// retrieveResponse retrieves the response saved by saveResponse.
func (c *Client) retrieveResponse(id string) (data []Response, err error) {
	resp, _ := c.responseNotifier.Load(id)
	if err = <-resp.(chan error); err == nil {
		data = c.getCurrentResults(id)
	}
	c.cleanResults(id, resp.(chan error), nil)
	return
}

func (c *Client) getCurrentResults(id string) (data []Response) {
	dataI, ok := c.results.Load(id)
	if !ok {
		return
	}
	d := dataI.([]interface{})
	dataI = nil
	data = make([]Response, len(d))
	if len(d) == 0 {
		return
	}
	for i := range d {
		data[i] = d[i].(Response)
	}
	return
}

func (c *Client) cleanResults(id string, respNotifier chan error, chunkNotifier chan bool) {
	if respNotifier == nil {
		return
	}
	c.responseNotifier.Delete(id)
	close(respNotifier)
	if chunkNotifier != nil {
		close(chunkNotifier)
		c.chunkNotifier.Delete(id)
	}
	c.deleteResponse(id)
}

// retrieveResponseCtx retrieves the response saved by saveResponse.
func (c *Client) retrieveResponseCtx(ctx context.Context, id string) (data []Response, err error) {
	respNotifier, _ := c.responseNotifier.Load(id)
	select {
	case err = <-respNotifier.(chan error):
		defer c.cleanResults(id, respNotifier.(chan error), nil)
		if err != nil {
			return
		}
		data = c.getCurrentResults(id)
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

// retrieveNextResponseCtx retrieves the current response (may be empty!) saved by saveResponse,
//  `done` is true when the results are complete (eof)
func (c *Client) retrieveNextResponseCtx(ctx context.Context, cursor *Cursor) (data []Response, done bool, err error) {
	c.Lock()
	respNotifier, ok := c.responseNotifier.Load(cursor.ID)
	c.Unlock()
	if respNotifier == nil || !ok {
		return
	}

	var chunkNotifier chan bool
	if chunkNotifierInterface, ok := c.chunkNotifier.Load(cursor.ID); ok {
		chunkNotifier = chunkNotifierInterface.(chan bool)
	}

	select {
	case err = <-respNotifier.(chan error):
		defer c.cleanResults(cursor.ID, respNotifier.(chan error), chunkNotifier)
		if err != nil {
			return
		}
		data = c.getCurrentResults(cursor.ID)
		done = true
	case <-chunkNotifier:
		c.Lock()
		data = c.getCurrentResults(cursor.ID)
		c.deleteResponse(cursor.ID)
		c.Unlock()
	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}

// deleteResponse deletes the response from the container. Used for cleanup purposes by requester.
func (c *Client) deleteResponse(id string) {
	c.results.Delete(id)
	return
}

// detectError detects any possible errors in responses from Gremlin Server and generates an error for each code
func (r *Response) detectError() (err error) {
	switch r.Status.Code {
	case StatusSuccess, StatusNoContent, StatusPartialContent:
	case StatusUnauthorized:
		err = fmt.Errorf("UNAUTHORIZED - Response Message: %s", r.Status.Message)
	case StatusAuthenticate:
		err = fmt.Errorf("AUTHENTICATE - Response Message: %s", r.Status.Message)
	case StatusMalformedRequest:
		err = fmt.Errorf("MALFORMED REQUEST - Response Message: %s", r.Status.Message)
	case StatusInvalidRequestArguments:
		err = fmt.Errorf("INVALID REQUEST ARGUMENTS - Response Message: %s", r.Status.Message)
	case StatusServerError:
		err = fmt.Errorf("SERVER ERROR - Response Message: %s", r.Status.Message)
	case StatusScriptEvaluationError:
		err = fmt.Errorf("SCRIPT EVALUATION ERROR - Response Message: %s", r.Status.Message)
	case StatusServerTimeout:
		err = fmt.Errorf("SERVER TIMEOUT - Response Message: %s", r.Status.Message)
	case StatusServerSerializationError:
		err = fmt.Errorf("SERVER SERIALIZATION ERROR - Response Message: %s", r.Status.Message)
	default:
		err = fmt.Errorf("UNKNOWN ERROR - Response Message: %s", r.Status.Message)
	}
	return
}
