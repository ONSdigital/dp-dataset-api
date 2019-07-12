package gremgo

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/gedge/graphson"
	"github.com/pkg/errors"
)

const connRequestQueueSize = 1000000

// errors
var (
	ErrGraphDBClosed = errors.New("graphdb is closed")
	ErrBadConn       = errors.New("bad conn")
)

// Pool maintains a list of connections.
type Pool struct {
	MaxOpen      int
	MaxLifetime  time.Duration
	dial         func() (*Client, error)
	mu           sync.Mutex
	freeConns    []*conn
	open         int
	openerCh     chan struct{}
	connRequests map[uint64]chan connRequest
	nextRequest  uint64
	cleanerCh    chan struct{}
	closed       bool
}

// NewPool create ConnectionPool
func NewPool(dial func() (*Client, error)) *Pool {
	p := new(Pool)
	p.dial = dial
	p.openerCh = make(chan struct{}, connRequestQueueSize)
	p.connRequests = make(map[uint64]chan connRequest)

	go p.opener()

	return p
}

// NewPoolWithDialerCtx returns a NewPool that uses a contextual dialer to dbURL,
// errs is a chan that receives any errors from the ping/read/write workers for the connection
func NewPoolWithDialerCtx(ctx context.Context, dbURL string, errs chan error, cfgs ...DialerConfig) *Pool {
	dialFunc := func() (*Client, error) {
		dialer := NewDialer(dbURL, cfgs...)
		cli, err := DialCtx(ctx, dialer, errs)
		return &cli, err
	}
	return NewPool(dialFunc)
}

type connRequest struct {
	*conn
	err error
}

// conn represents a shared and reusable connection.
type conn struct {
	Pool   *Pool
	Client *Client
	t      time.Time
}

// maybeOpenNewConnections initiates new connections if capacity allows (must be locked)
func (p *Pool) maybeOpenNewConnections() {
	if p.closed {
		return
	}
	numRequests := len(p.connRequests)
	if p.MaxOpen > 0 {
		numCanOpen := p.MaxOpen - p.open
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		p.open++
		numRequests--
		p.openerCh <- struct{}{}
	}
}

func (p *Pool) opener() {
	for range p.openerCh {
		if err := p.openNewConnection(); err != nil {
			// gutil.WarnLev(1, "failed opener "+err.Error()) XXX
		}
	}
}

type so struct {
	tryOpening    bool
	alreadyLocked bool
	conn          *conn
}

// subtractOpen reduces p.open (count), unlocks. Optionally: locks, maybeOpenNewConnections, conn.Client.Close
func (p *Pool) subtractOpen(opts so, err error) error {
	if !opts.alreadyLocked {
		p.mu.Lock()
	}
	p.open--
	if opts.tryOpening {
		p.maybeOpenNewConnections()
	}
	p.mu.Unlock()
	if opts.conn != nil {
		opts.conn.Client.Close()
	}
	return err
}

func (p *Pool) openNewConnection() (err error) {
	if p.closed {
		return p.subtractOpen(so{}, errors.Errorf("failed to openNewConnection - pool closed"))
	}
	var c *Client
	c, err = p.dial()
	if err != nil {
		return p.subtractOpen(so{tryOpening: true}, errors.Wrapf(err, "failed to openNewConnection - dial"))
	}
	cn := &conn{
		Pool:   p,
		Client: c,
		t:      time.Now(),
	}
	p.mu.Lock()
	if !p.putConnLocked(cn, nil) {
		return p.subtractOpen(so{alreadyLocked: true, conn: cn}, errors.Errorf("failed to openNewConnection - connLocked"))
	}
	p.mu.Unlock()
	return
}

// putConn releases a connection back to the connection pool.
func (p *Pool) putConn(cn *conn, err error) error {
	p.mu.Lock()
	if !p.putConnLocked(cn, err) {
		return p.subtractOpen(so{alreadyLocked: true, conn: cn}, err)
	}
	p.mu.Unlock()
	return err
}

// putConnLocked releases a connection back to the connection pool (must be locked)
// returns false when unable to do so (pool is closed, open is at max)
func (p *Pool) putConnLocked(cn *conn, err error) bool {
	if p.closed {
		return false
	}
	if p.MaxOpen > 0 && p.open >= p.MaxOpen {
		return false
	}
	if len(p.connRequests) > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range p.connRequests {
			break
		}
		delete(p.connRequests, reqKey)
		req <- connRequest{
			conn: cn,
			err:  err,
		}
	} else {
		p.freeConns = append(p.freeConns, cn)
		p.startCleanerLocked()
	}
	return true
}

// conn will return an available pooled connection. Either an idle connection or
// by dialing a new one if the pool does not currently have a maximum number
// of active connections.
func (p *Pool) conn() (*conn, error) {
	ctx := context.Background()
	return p.connCtx(ctx)
}
func (p *Pool) connCtx(ctx context.Context) (*conn, error) {
	cn, err := p._conn(ctx, true)
	if err == nil {
		return cn, nil
	}
	if errors.Cause(err) == ErrBadConn {
		return p._conn(ctx, false)
	}
	return cn, err
}

func (p *Pool) _conn(ctx context.Context, useFreeConn bool) (*conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrGraphDBClosed
	}
	// Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		p.mu.Unlock()
		return nil, errors.Wrap(ctx.Err(), "the context is expired")
	}

	var pc *conn
	numFree := len(p.freeConns)
	if useFreeConn && numFree > 0 {
		pc = p.freeConns[0]
		copy(p.freeConns, p.freeConns[1:])
		p.freeConns = p.freeConns[:numFree-1]
		p.mu.Unlock()
		if pc.expired(p.MaxLifetime) {
			return nil, p.subtractOpen(so{conn: pc}, ErrBadConn)
		}
		return pc, nil
	}

	if p.MaxOpen > 0 && p.MaxOpen <= p.open {
		req := make(chan connRequest, 1)
		reqKey := p.nextRequest
		p.nextRequest++
		p.connRequests[reqKey] = req
		p.mu.Unlock()

		select {
		// timeout
		case <-ctx.Done():
			// Remove the connection request and ensure no value has been sent
			// on it after removing.
			p.mu.Lock()
			delete(p.connRequests, reqKey)
			p.mu.Unlock()
			select {
			case ret, ok := <-req:
				if ok {
					p.putConn(ret.conn, ret.err)
				}
			default:
			}
			return nil, errors.Wrap(ctx.Err(), "Deadline of connRequests exceeded")
		case ret, ok := <-req:
			if !ok {
				return nil, ErrGraphDBClosed
			}
			if ret.err != nil {
				return ret.conn, errors.Wrap(ret.err, "Response has an error")
			}
			return ret.conn, nil
		}
	}

	p.open++
	p.mu.Unlock()
	newCn, err := p.dial()
	if err != nil {
		return nil, p.subtractOpen(so{tryOpening: true}, errors.Wrap(err, "Failed newConn"))
	}
	return &conn{
		Pool:   p,
		Client: newCn,
		t:      time.Now(),
	}, nil
}

func (p *Pool) needStartCleaner() bool {
	return p.MaxLifetime > 0 &&
		p.open > 0 &&
		p.cleanerCh == nil
}

// startCleanerLocked starts connectionCleaner if needed.
func (p *Pool) startCleanerLocked() {
	if p.needStartCleaner() {
		p.cleanerCh = make(chan struct{}, 1)
		go p.connectionCleaner()
	}
}

func (p *Pool) connectionCleaner() {
	const minInterval = time.Second

	d := p.MaxLifetime
	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-p.cleanerCh: // dbclient was closed.
		}

		ml := p.MaxLifetime
		p.mu.Lock()
		if p.closed || len(p.freeConns) == 0 || ml <= 0 {
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}
		n := time.Now()
		mlExpiredSince := n.Add(-ml)
		var closing []*conn
		for i := 0; i < len(p.freeConns); i++ {
			pc := p.freeConns[i]
			if (ml > 0 && pc.t.Before(mlExpiredSince)) ||
				pc.Client.Errored {
				p.open--
				closing = append(closing, pc)
				last := len(p.freeConns) - 1
				p.freeConns[i] = p.freeConns[last]
				p.freeConns[last] = nil
				p.freeConns = p.freeConns[:last]
				i--
			}
		}
		p.mu.Unlock()

		for _, pc := range closing {
			if pc.Client != nil {
				pc.Client.Close()
			}
		}

		t.Reset(d)
	}
}

// Execute formats a raw Gremlin query, sends it to Gremlin Server, and returns the result.
func (p *Pool) Execute(query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	return p.ExecuteCtx(context.Background(), query, bindings, rebindings)
}
func (p *Pool) ExecuteCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	pc, err := p.conn()
	if err != nil {
		return resp, errors.Wrap(err, "Failed p.conn")
	}
	defer func() {
		p.putConn(pc, err)
	}()
	resp, err = pc.Client.executeRequestCtx(ctx, query, bindings, rebindings)
	return
}

// ExecuteFile takes a file path to a Gremlin script, sends it to Gremlin Server, and returns the result.
func (p *Pool) ExecuteFile(path string, bindings, rebindings map[string]string) (resp []Response, err error) {
	pc, err := p.conn()
	if err != nil {
		return resp, errors.Wrap(err, "Failed p.conn")
	}
	defer func() {
		p.putConn(pc, err)
	}()
	d, err := ioutil.ReadFile(path) // Read script from file
	if err != nil {
		log.Println(err)
		return
	}
	query := string(d)
	resp, err = pc.Client.executeRequest(query, bindings, rebindings)
	return
}

// AddV
func (p *Pool) AddV(label string, i interface{}, bindings, rebindings map[string]string) (resp graphson.Vertex, err error) {
	return p.AddVertexCtx(context.Background(), label, i, bindings, rebindings)
}
func (p *Pool) AddVertexCtx(ctx context.Context, label string, i interface{}, bindings, rebindings map[string]string) (resp graphson.Vertex, err error) {
	var pc *conn
	if pc, err = p.conn(); err != nil {
		return resp, errors.Wrap(err, "Failed p.conn")
	}
	defer p.putConn(pc, err)
	return pc.Client.AddVertexCtx(ctx, label, i, bindings, rebindings)
}

// Get
func (p *Pool) Get(query string, bindings, rebindings map[string]string) (resp interface{}, err error) {
	var pc *conn
	if pc, err = p.conn(); err != nil {
		return resp, errors.Wrap(err, "Failed p.conn")
	}
	defer p.putConn(pc, err)
	return pc.Client.Get(query, bindings, rebindings)
}

// GetCtx
func (p *Pool) GetCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (resp interface{}, err error) {
	var pc *conn
	if pc, err = p.connCtx(ctx); err != nil {
		return resp, errors.Wrap(err, "GetCtx: Failed p.connCtx")
	}
	defer p.putConn(pc, err)
	return pc.Client.GetCtx(ctx, query, bindings, rebindings)
}

// OpenCursorCtx initiates a query on the database, returning a cursor to iterate over the results
func (p *Pool) OpenCursorCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (cursor *Cursor, err error) {
	var pc *conn
	if pc, err = p.connCtx(ctx); err != nil {
		err = errors.Wrap(err, "GetCursorCtx: Failed p.connCtx")
		return
	}
	defer p.putConn(pc, err)
	return pc.Client.OpenCursorCtx(ctx, query, bindings, rebindings)
}

// ReadCursorCtx returns the next set of results for the cursor
// - `res` returns vertices (and may be empty when results were read by a previous call - this is normal)
// - `eof` will be true when no more results are available (`res` may still have results)
func (p *Pool) ReadCursorCtx(ctx context.Context, cursor *Cursor) (res []graphson.Vertex, eof bool, err error) {
	var pc *conn
	if pc, err = p.connCtx(ctx); err != nil {
		err = errors.Wrap(err, "NextCtx: Failed p.connCtx")
		return
	}
	defer p.putConn(pc, err)
	return pc.Client.ReadCursorCtx(ctx, cursor)
}

// AddE
func (p *Pool) AddE(label, fromId, toId string, props map[string]interface{}) (resp interface{}, err error) {
	return p.AddEdgeCtx(context.Background(), label, fromId, toId, props)
}

func (p *Pool) AddEdgeCtx(ctx context.Context, label, fromId, toId string, props map[string]interface{}) (resp interface{}, err error) {
	// AddEdgeCtx
	var pc *conn
	if pc, err = p.conn(); err != nil {
		return resp, errors.Wrap(err, "Failed p.conn")
	}
	defer p.putConn(pc, err)
	return pc.Client.AddEdgeCtx(ctx, label, fromId, toId, props)
}

// GetE
func (p *Pool) GetE(q string, bindings, rebindings map[string]string) (resp interface{}, err error) {
	return p.GetEdgeCtx(context.Background(), q, bindings, rebindings)
}

func (p *Pool) GetEdgeCtx(ctx context.Context, q string, bindings, rebindings map[string]string) (resp interface{}, err error) {
	var pc *conn
	if pc, err = p.conn(); err != nil {
		return resp, errors.Wrap(err, "Failed p.conn")
	}
	defer p.putConn(pc, err)
	return pc.Client.GetEdgeCtx(ctx, q, bindings, rebindings)
}

func (p *Pool) GetCount(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return p.GetCountCtx(context.Background(), q, bindings, rebindings)
}
func (p *Pool) GetCountCtx(ctx context.Context, q string, bindings, rebindings map[string]string) (i int64, err error) {
	var pc *conn
	if pc, err = p.conn(); err != nil {
		return 0, errors.Wrap(err, "Failed p.conn")
	}
	defer p.putConn(pc, err)
	return pc.Client.GetCountCtx(ctx, q, bindings, rebindings)
}

func (p *Pool) GetStringList(q string, bindings, rebindings map[string]string) (vals []string, err error) {
	return p.GetStringListCtx(context.Background(), q, bindings, rebindings)
}
func (p *Pool) GetStringListCtx(ctx context.Context, q string, bindings, rebindings map[string]string) (vals []string, err error) {
	var pc *conn
	if pc, err = p.conn(); err != nil {
		err = errors.Wrap(err, "GetStringListCtx: Failed p.conn")
		return
	}
	defer p.putConn(pc, err)
	return pc.Client.GetStringListCtx(ctx, q, bindings, rebindings)
}

// GetProperties returns a map of vertex properties
func (p *Pool) GetProperties(q string, bindings, rebindings map[string]string) (vals map[string][]interface{}, err error) {
	return p.GetPropertiesCtx(context.Background(), q, bindings, rebindings)
}
func (p *Pool) GetPropertiesCtx(ctx context.Context, q string, bindings, rebindings map[string]string) (vals map[string][]interface{}, err error) {
	var pc *conn
	if pc, err = p.conn(); err != nil {
		err = errors.Wrap(err, "GetPropertiesCtx: Failed p.conn")
		return
	}
	defer p.putConn(pc, err)
	return pc.Client.GetPropertiesCtx(ctx, q, bindings, rebindings)
}

// Close closes the pool.
func (p *Pool) Close() {
	p.mu.Lock()

	close(p.openerCh)
	if p.cleanerCh != nil {
		close(p.cleanerCh)
	}
	for _, cr := range p.connRequests {
		close(cr)
	}
	p.closed = true
	p.mu.Unlock()
	for _, pc := range p.freeConns {
		if pc.Client != nil {
			pc.Client.Close()
		}
	}
	p.mu.Lock()
	p.freeConns = nil
	p.mu.Unlock()
}

func (cn *conn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return cn.t.Add(timeout).Before(time.Now())
}
