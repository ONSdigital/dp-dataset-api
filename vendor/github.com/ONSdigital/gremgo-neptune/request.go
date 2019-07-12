package gremgo

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/gofrs/uuid"
)

const mimeTypeStr = "application/vnd.gremlin-v3.0+json"

// create the header as []byte with the length byte as prefix
var mimeTypePrefix = append([]byte{byte(len(mimeTypeStr))}, []byte(mimeTypeStr)...)

type requester interface {
	prepare() error
	getID() string
	getRequest() request
}

// request is a container for all evaluation request parameters to be sent to the Gremlin Server.
type request struct {
	RequestID string                 `json:"requestId"`
	Op        string                 `json:"op"`
	Processor string                 `json:"processor"`
	Args      map[string]interface{} `json:"args"`
}

// prepareRequest packages a query and binding into the format that Gremlin Server accepts
func prepareRequest(query string, bindings, rebindings map[string]string) (req request, id string, err error) {
	var uuID uuid.UUID
	if uuID, err = uuid.NewV4(); err != nil {
		return
	}
	id = uuID.String()

	req.RequestID = id
	req.Op = "eval"
	req.Processor = ""

	req.Args = make(map[string]interface{})
	req.Args["language"] = "gremlin-groovy"
	req.Args["gremlin"] = query
	if len(bindings) > 0 || len(rebindings) > 0 {
		req.Args["bindings"] = bindings
		req.Args["rebindings"] = rebindings
	}

	return
}

//prepareAuthRequest creates a ws request for Gremlin Server
func prepareAuthRequest(requestID string, username string, password string) (req request, err error) {
	req.RequestID = requestID
	req.Op = "authentication"
	req.Processor = "trasversal"

	var simpleAuth []byte
	user := []byte(username)
	pass := []byte(password)

	simpleAuth = append(simpleAuth, 0)
	simpleAuth = append(simpleAuth, user...)
	simpleAuth = append(simpleAuth, 0)
	simpleAuth = append(simpleAuth, pass...)

	req.Args = make(map[string]interface{})
	req.Args["sasl"] = base64.StdEncoding.EncodeToString(simpleAuth)

	return
}

// packageRequest takes a request type and formats it into being able to be delivered to Gremlin Server
func packageRequest(req request) (msg []byte, err error) {
	j, err := json.Marshal(req) // Formats request into byte format
	if err != nil {
		return
	}
	return append(mimeTypePrefix, j...), nil
}

// dispatchRequest sends the request for writing to the remote Gremlin Server
func (c *Client) dispatchRequest(msg []byte) {
	c.requests <- msg
}

// dispatchRequestCtx sends the request for writing to the remote Gremlin Server
func (c *Client) dispatchRequestCtx(ctx context.Context, msg []byte) (err error) {
	select {
	case c.requests <- msg:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}
