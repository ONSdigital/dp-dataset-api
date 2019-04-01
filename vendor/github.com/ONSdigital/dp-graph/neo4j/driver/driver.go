package driver

import (
	"context"
	"io"

	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

//go:generate moq -out ../internal/driver.go -pkg internal . Neo4jDriver
//go:generate moq -out ../internal/bolt.go -pkg internal . Result

// Result of queries to neo4j - needed for mocking and tests
type Result bolt.Result

// Neo4jDriver is the interface that wraps basic neo4j driver functionality
type Neo4jDriver interface {
	Count(query string) (count int64, err error)
	Exec(query string, params map[string]interface{}) (bolt.Result, error)
	Read(query string, mapp mapper.ResultMapper, single bool) error
	ReadWithParams(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error
	StreamRows(query string) (*BoltRowReader, error)

	driver.Driver
}

// NeoDriver contains a connection pool and allows basic interaction with the database
type NeoDriver struct {
	pool bolt.ClosableDriverPool
}

// New neo4j closeable connection pool configured with the provided address,
// connection pool size and timeout
func New(dbAddr string, size, timeout int) (n *NeoDriver, err error) {
	pool, err := bolt.NewClosableDriverPoolWithTimeout(dbAddr, size, timeout)
	if err != nil {
		return nil, err
	}

	return &NeoDriver{
		pool: pool,
	}, nil
}

//Close the contained connection pool
func (n *NeoDriver) Close(ctx context.Context) error {
	return n.pool.Close()
}

// ReadWithParams takes a query, a map of parameters and an indicator of whether
// a single or list response is expected and writes results into the ResultMapper provided
func (n *NeoDriver) ReadWithParams(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
	return n.read(query, params, mapp, single)
}

// ReadWithParams takes a query and an indicator of whether a single or list
// response is expected and writes results into the ResultMapper provided
func (n *NeoDriver) Read(query string, mapp mapper.ResultMapper, single bool) error {
	return n.read(query, nil, mapp, single)
}

func (n *NeoDriver) read(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
	c, err := n.pool.OpenPool()
	if err != nil {
		return err
	}
	defer c.Close()

	rows, err := c.QueryNeo(query, params)
	if err != nil {
		return errors.WithMessage(err, "error executing neo4j query")
	}
	defer rows.Close()

	index := 0
	numOfResults := 0
results:
	for {
		data, meta, nextNeoErr := rows.NextNeo()
		if nextNeoErr != nil {
			if nextNeoErr != io.EOF {
				return errors.WithMessage(nextNeoErr, "extractResults: rows.NextNeo() return unexpected error")
			}
			break results
		}

		numOfResults++
		if single && index > 0 {
			return errors.WithMessage(err, "non unique results")
		}

		if mapp != nil {
			if err := mapp(&mapper.Result{Data: data, Meta: meta, Index: index}); err != nil {
				return errors.WithMessage(err, "mapResult returned an error")
			}
		}
		index++
	}

	if numOfResults == 0 {
		return driver.ErrNotFound
	}

	return nil
}

// StreamRows according to provided query into a Reader and return the Reader and any errors.
// The Reader will contain reference to the database connection and must be closed
// by the caller.
func (n *NeoDriver) StreamRows(query string) (*BoltRowReader, error) {
	conn, err := n.pool.OpenPool()
	if err != nil {
		return nil, err
	}

	rows, err := conn.QueryNeo(query, nil)
	if err != nil {
		// Before returning the error "close" the open connection to release it back into the pool.
		conn.Close()
		return nil, err
	}

	// The connection can only be closed once the results have been read, so the caller is responsible for
	// calling .Close() which will ultimately release the connection back into the pool
	return NewBoltRowReader(rows, conn), nil
}

// Count nodes returned by the provided query
func (n *NeoDriver) Count(query string) (count int64, err error) {
	c, err := n.pool.OpenPool()
	if err != nil {
		return
	}
	defer c.Close()

	rows, err := c.QueryNeo(query, nil)
	if err != nil {
		err = errors.WithMessage(err, "error executing neo4j query")
		return
	}
	defer rows.Close()

	data, _, err := rows.All()
	if err != nil {
		return
	}

	var ok bool
	if count, ok = data[0][0].(int64); !ok {
		err = errors.New("Could not get result from DB")
	}

	return
}

// Exec executes the provided query with relevant parameters and returns the response directly
func (n *NeoDriver) Exec(query string, params map[string]interface{}) (bolt.Result, error) {
	c, err := n.pool.OpenPool()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	return c.ExecNeo(query, params)
}
