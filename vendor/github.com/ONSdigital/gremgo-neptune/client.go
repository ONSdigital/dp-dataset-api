package gremgo

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/gedge/graphson"
	"github.com/pkg/errors"
)

var (
	ErrorConnectionDisposed      = errors.New("you cannot write on a disposed connection")
	ErrorNoGraphTags             = errors.New("does not contain any graph tags")
	ErrorUnsupportedPropertyType = errors.New("unsupported property map value type")
)

// Client is a container for the gremgo client.
type Client struct {
	conn             dialer
	requests         chan []byte
	responses        chan []byte
	results          *sync.Map
	responseNotifier *sync.Map // responseNotifier notifies the requester that a response has been completed for the request
	chunkNotifier    *sync.Map // chunkNotifier contains channels per requestID (if using cursors) which notifies the requester that a partial response has arrived
	sync.RWMutex
	Errored bool
}

// NewDialer returns a WebSocket dialer to use when connecting to Gremlin Server
func NewDialer(host string, configs ...DialerConfig) (dialer *Ws) {
	dialer = &Ws{
		timeout:      15 * time.Second,
		pingInterval: 60 * time.Second,
		writingWait:  15 * time.Second,
		readingWait:  15 * time.Second,
		connected:    false,
		quit:         make(chan struct{}),
	}

	for _, conf := range configs {
		conf(dialer)
	}

	dialer.host = host
	return dialer
}

func newClient() (c Client) {
	c.requests = make(chan []byte, 3)  // c.requests takes any request and delivers it to the WriteWorker for dispatch to Gremlin Server
	c.responses = make(chan []byte, 3) // c.responses takes raw responses from ReadWorker and delivers it for sorting to handleResponse
	c.results = &sync.Map{}
	c.responseNotifier = &sync.Map{}
	c.chunkNotifier = &sync.Map{}
	return
}

// Dial returns a gremgo client for interaction with the Gremlin Server specified in the host IP.
func Dial(conn dialer, errs chan error) (c Client, err error) {
	return DialCtx(context.Background(), conn, errs)
}

// DialCtx returns a gremgo client for interaction with the Gremlin Server specified in the host IP.
func DialCtx(ctx context.Context, conn dialer, errs chan error) (c Client, err error) {
	c = newClient()
	c.conn = conn

	// Connects to Gremlin Server
	err = conn.connectCtx(ctx)
	if err != nil {
		return
	}

	msgChan := make(chan []byte, 200)

	go c.writeWorkerCtx(ctx, errs)
	go c.readWorkerCtx(ctx, msgChan, errs)
	go c.saveWorkerCtx(ctx, msgChan, errs)
	go conn.pingCtx(ctx, errs)

	return
}

func (c *Client) executeRequest(query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	return c.executeRequestCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) executeRequestCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	var req request
	var id string
	req, id, err = prepareRequest(query, bindings, rebindings)
	if err != nil {
		return
	}

	msg, err := packageRequest(req)
	if err != nil {
		log.Println(err)
		return
	}
	c.responseNotifier.Store(id, make(chan error, 1))
	c.dispatchRequestCtx(ctx, msg)
	resp, err = c.retrieveResponseCtx(ctx, id)
	if err != nil {
		err = errors.Wrapf(err, "query: %s", query)
	}
	return
}
func (c *Client) executeRequestCursorCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (cursor *Cursor, err error) {
	var req request
	var id string
	if req, id, err = prepareRequest(query, bindings, rebindings); err != nil {
		return
	}

	var msg []byte
	if msg, err = packageRequest(req); err != nil {
		log.Println(err)
		return
	}
	c.responseNotifier.Store(id, make(chan error, 1))
	c.chunkNotifier.Store(id, make(chan bool, 10))
	if c.dispatchRequestCtx(ctx, msg); err != nil {
		err = errors.Wrap(err, "executeRequestCursorCtx")
		return
	}

	cursor = &Cursor{
		ID:     id,
		client: c,
	}
	return
}

func (c *Client) authenticate(requestID string) (err error) {
	auth := c.conn.getAuth()
	req, err := prepareAuthRequest(requestID, auth.username, auth.password)
	if err != nil {
		return
	}

	msg, err := packageRequest(req)
	if err != nil {
		log.Println(err)
		return
	}

	c.dispatchRequest(msg)
	return
}

// Execute formats a raw Gremlin query, sends it to Gremlin Server, and returns the result.
func (c *Client) Execute(query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	return c.ExecuteCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) ExecuteCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	if c.conn.IsDisposed() {
		return resp, ErrorConnectionDisposed
	}
	return c.executeRequestCtx(ctx, query, bindings, rebindings)
}

// ExecuteFile takes a file path to a Gremlin script, sends it to Gremlin Server, and returns the result.
func (c *Client) ExecuteFile(path string, bindings, rebindings map[string]string) (resp []Response, err error) {
	if c.conn.IsDisposed() {
		return resp, ErrorConnectionDisposed
	}
	d, err := ioutil.ReadFile(path) // Read script from file
	if err != nil {
		log.Println(err)
		return
	}
	query := string(d)
	return c.executeRequest(query, bindings, rebindings)
}

// Get formats a raw Gremlin query, sends it to Gremlin Server, and populates the passed []interface.
func (c *Client) Get(query string, bindings, rebindings map[string]string) (res []graphson.Vertex, err error) {
	return c.GetCtx(context.Background(), query, bindings, rebindings)
}

// GetCtx - execute a gremlin command and return the response as vertices
func (c *Client) GetCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (res []graphson.Vertex, err error) {
	if c.conn.IsDisposed() {
		err = ErrorConnectionDisposed
		return
	}

	var resp []Response
	resp, err = c.executeRequestCtx(ctx, query, bindings, rebindings)
	if err != nil {
		return
	}
	return c.deserializeResponseToVertices(resp)
}

func (c *Client) deserializeResponseToVertices(resp []Response) (res []graphson.Vertex, err error) {
	if len(resp) == 0 || resp[0].Status.Code == statusNoContent {
		return
	}

	for _, item := range resp {
		resN, err := graphson.DeserializeListOfVerticesFromBytes(item.Result.Data)
		if err != nil {
			panic(err)
		}
		res = append(res, resN...)
	}
	return
}

// OpenCursorCtx initiates a query on the database, returning a cursor used to iterate over the results as they arrive
func (c *Client) OpenCursorCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (cursor *Cursor, err error) {
	if c.conn.IsDisposed() {
		err = ErrorConnectionDisposed
		return
	}
	return c.executeRequestCursorCtx(ctx, query, bindings, rebindings)
}

// ReadCursorCtx returns the next set of results, deserialized as []Vertex, for the cursor
// - `res` may be empty when results were read by a previous call
// - `eof` will be true when no more results are available
func (c *Client) ReadCursorCtx(ctx context.Context, cursor *Cursor) (res []graphson.Vertex, eof bool, err error) {
	var resp []Response
	if resp, eof, err = c.retrieveNextResponseCtx(ctx, cursor); err != nil {
		err = errors.Wrapf(err, "ReadCursorCtx: %s", cursor.ID)
		return
	}

	if res, err = c.deserializeResponseToVertices(resp); err != nil {
		err = errors.Wrapf(err, "ReadCursorCtx: %s", cursor.ID)
		return
	}
	return
}

// GetE formats a raw Gremlin query, sends it to Gremlin Server, and populates the passed []interface.
func (c *Client) GetE(query string, bindings, rebindings map[string]string) (res []graphson.Edge, err error) {
	return c.GetEdgeCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) GetEdgeCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (res []graphson.Edge, err error) {
	if c.conn.IsDisposed() {
		err = ErrorConnectionDisposed
		return
	}

	resp, err := c.executeRequestCtx(ctx, query, bindings, rebindings)
	if err != nil {
		return
	}
	if len(resp) == 0 || resp[0].Status.Code == statusNoContent {
		return
	}

	for _, item := range resp {
		var resN []graphson.Edge
		if resN, err = graphson.DeserializeListOfEdgesFromBytes(item.Result.Data); err != nil {
			return
		}
		res = append(res, resN...)
	}
	return
}

// GetCount returns the count element returned by an Execute()
func (c *Client) GetCount(query string, bindings, rebindings map[string]string) (i int64, err error) {
	return c.GetCountCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) GetCountCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (i int64, err error) {
	var res []Response
	if res, err = c.ExecuteCtx(ctx, query, bindings, rebindings); err != nil {
		return
	}
	if len(res) > 1 {
		err = errors.New("GetCount: expected one result, got more than one")
		return
	} else if len(res) == 0 {
		err = errors.New("GetCount: expected one result, got zero")
		return
	}
	if i, err = graphson.DeserializeNumber(res[0].Result.Data); err != nil {
		return
	}
	return
}

// GetStringList returns the list of string elements returned by an Execute() (e.g. from `...().properties('p').value()`)
func (c *Client) GetStringList(query string, bindings, rebindings map[string]string) (vals []string, err error) {
	return c.GetStringListCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) GetStringListCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (vals []string, err error) {
	var res []Response
	if res, err = c.ExecuteCtx(ctx, query, bindings, rebindings); err != nil {
		return
	}
	for _, resN := range res {
		var valsN []string
		if valsN, err = graphson.DeserializeStringListFromBytes(resN.Result.Data); err != nil {
			return
		}
		vals = append(vals, valsN...)
	}
	return
}

// GetProperties returns a map of string to interface{} returned by an Execute() for vertex .properties()
func (c *Client) GetProperties(query string, bindings, rebindings map[string]string) (vals map[string][]interface{}, err error) {
	return c.GetPropertiesCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) GetPropertiesCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (vals map[string][]interface{}, err error) {
	var res []Response
	if res, err = c.ExecuteCtx(ctx, query, bindings, rebindings); err != nil {
		return
	}
	vals = make(map[string][]interface{})
	for _, resN := range res {
		if err = graphson.DeserializePropertiesFromBytes(resN.Result.Data, vals); err != nil {
			return
		}
	}
	return
}

// GremlinForVertex returns the addV()... and V()... gremlin commands for `data`
// Because of possible multiples, it does not start with `g.` (it probably should? XXX )
// (largely taken from https://github.com/intwinelabs/gremgoser)
func GremlinForVertex(label string, data interface{}) (gremAdd, gremGet string, err error) {
	gremAdd = fmt.Sprintf("addV('%s')", label)
	gremGet = fmt.Sprintf("V('%s')", label)

	d := reflect.ValueOf(data)
	id := d.FieldByName("Id")
	if id.IsValid() {
		if idField, ok := d.Type().FieldByName("Id"); ok {
			tag := idField.Tag.Get("graph")
			name, opts := parseTag(tag)
			if len(name) == 0 && len(opts) == 0 {
				gremAdd += fmt.Sprintf(".property(id,'%s')", id)
				gremGet += fmt.Sprintf(".hasId('%s')", id)
			}
		}
	}

	missingTag := true

	for i := 0; i < d.NumField(); i++ {
		tag := d.Type().Field(i).Tag.Get("graph")
		name, opts := parseTag(tag)
		if (len(name) == 0 || name == "-") && len(opts) == 0 {
			continue
		}
		missingTag = false
		val := d.Field(i).Interface()
		if len(opts) == 0 {
			err = fmt.Errorf("interface field tag %q does not contain a tag option type, field type: %T", name, val)
			return
		}
		if !d.Field(i).IsValid() {
			continue
		}
		if opts.Contains("id") {
			if val != "" {
				gremAdd += fmt.Sprintf(".property(id,'%s')", val)
				gremGet += fmt.Sprintf(".hasId('%s')", val)
			}
		} else if opts.Contains("string") {
			if val != "" {
				gremAdd += fmt.Sprintf(".property('%s','%s')", name, escapeStringy(val.(string)))
				gremGet += fmt.Sprintf(".has('%s','%s')", name, escapeStringy(val))
			}
		} else if opts.Contains("bool") || opts.Contains("number") || opts.Contains("other") {
			gremAdd += fmt.Sprintf(".property('%s',%v)", name, val)
			gremGet += fmt.Sprintf(".has('%s',%v)", name, val)
		} else if opts.Contains("[]string") {
			s := reflect.ValueOf(val)
			for i := 0; i < s.Len(); i++ {
				gremAdd += fmt.Sprintf(".property('%s','%s')", name, escapeStringy(s.Index(i).Interface()))
				gremGet += fmt.Sprintf(".has('%s','%s')", name, escapeStringy(s.Index(i).Interface()))
			}
		} else if opts.Contains("[]bool") || opts.Contains("[]number") || opts.Contains("[]other") {
			s := reflect.ValueOf(val)
			for i := 0; i < s.Len(); i++ {
				gremAdd += fmt.Sprintf(".property('%s',%v)", name, s.Index(i).Interface())
				gremGet += fmt.Sprintf(".has('%s',%v)", name, s.Index(i).Interface())
			}
		} else {
			err = fmt.Errorf("interface field tag needs recognised option, field: %q, tag: %q", d.Type().Field(i).Name, tag)
			return
		}
	}

	if missingTag {
		// this err is effectively a warning for gremGet (can be ignored, unless no Id)
		err = ErrorNoGraphTags
		return
	}
	return
}

// AddV takes a label and an interface and adds it as a vertex to the graph
func (c *Client) AddV(label string, data interface{}, bindings, rebindings map[string]string) (vert graphson.Vertex, err error) {
	return c.AddVertexCtx(context.Background(), label, data, bindings, rebindings)
}
func (c *Client) AddVertexCtx(ctx context.Context, label string, data interface{}, bindings, rebindings map[string]string) (vert graphson.Vertex, err error) {
	if c.conn.IsDisposed() {
		return vert, ErrorConnectionDisposed
	}

	q, _, err := GremlinForVertex(label, data)
	if err != nil && err != ErrorNoGraphTags {
		panic(err) // XXX
	}
	q = "g." + q

	var resp []Response
	if resp, err = c.ExecuteCtx(ctx, q, bindings, rebindings); err != nil {
		return
	}

	if len(resp) != 1 {
		return vert, fmt.Errorf("AddV should receive 1 response, got %d", len(resp))
	}

	for _, res := range resp { // XXX one result, so should not need loop
		var result []graphson.Vertex
		if result, err = graphson.DeserializeListOfVerticesFromBytes(res.Result.Data); err != nil {
			return
		}
		if len(result) != 1 {
			return vert, fmt.Errorf("AddV should receive 1 vertex, got %d", len(result))
		}

		vert = result[0]
	}
	return
}

// AddE takes a label, from UUID and to UUID (and optional props map) and creates an edge between the two vertex in the graph
func (c *Client) AddE(label, fromId, toId string, props map[string]interface{}) (resp interface{}, err error) {
	return c.AddEdgeCtx(context.Background(), label, fromId, toId, props)
}
func (c *Client) AddEdgeCtx(ctx context.Context, label, fromId, toId string, props map[string]interface{}) (resp interface{}, err error) {
	if c.conn.IsDisposed() {
		return nil, ErrorConnectionDisposed
	}

	var propStr string
	if propStr, err = buildProps(props); err != nil {
		return
	}
	q := fmt.Sprintf("g.addE('%s').from(g.V().hasId('%s')).to(g.V().hasId('%s'))%s", label, fromId, toId, propStr)
	resp, err = c.ExecuteCtx(ctx, q, nil, nil)
	return
}

// Close closes the underlying connection and marks the client as closed.
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.close()
	}
}

// buildProps converts a map[string]interfaces to be used as properties on an edge
// (largely taken from https://github.com/intwinelabs/gremgoser)
func buildProps(props map[string]interface{}) (q string, err error) {
	for k, v := range props {
		t := reflect.ValueOf(v).Kind()
		if t == reflect.String {
			q += fmt.Sprintf(".property('%s', '%s')", k, escapeStringy(v))
		} else if t == reflect.Bool || t == reflect.Int || t == reflect.Int8 || t == reflect.Int16 || t == reflect.Int32 || t == reflect.Int64 || t == reflect.Uint || t == reflect.Uint8 || t == reflect.Uint16 || t == reflect.Uint32 || t == reflect.Uint64 || t == reflect.Float32 || t == reflect.Float64 {
			q += fmt.Sprintf(".property('%s', %v)", k, v)
		} else if t == reflect.Slice {
			s := reflect.ValueOf(v)
			for i := 0; i < s.Len(); i++ {
				q += fmt.Sprintf(".property('%s', '%s')", k, escapeStringy(s.Index(i).Interface()))
			}
		} else {
			return "", ErrorUnsupportedPropertyType
		}
	}
	return
}

// escapeStringy takes a string and escapes some characters
// (largely taken from https://github.com/intwinelabs/gremgoser)
func escapeStringy(stringy interface{}) string {
	var buf bytes.Buffer
	for _, char := range stringy.(string) {
		switch char {
		case '\'', '"', '\\':
			buf.WriteRune('\\')
		}
		buf.WriteRune(char)
	}
	return buf.String()
}
