package driver

import (
	"context"
	"io"

	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

//go:generate moq -out ../internal/driver.go -pkg internal . Neo4jDriver

type Neo4jDriver interface {
	Read(query string, mapp mapper.ResultMapper, single bool) error
	ReadWithParams(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error
	StreamRows(query string) (*BoltRowReader, error)
	Count(query string) (count int64, err error)
	Exec(query string, params map[string]interface{}) (bolt.Result, error)
	Close(ctx context.Context) error
	Healthcheck() (string, error)
}

type NeoDriver struct {
	pool bolt.ClosableDriverPool
}

func New(dbAddr string, size, timeout int) (n *NeoDriver, err error) {
	pool, err := bolt.NewClosableDriverPoolWithTimeout(dbAddr, size, timeout)
	if err != nil {
		return nil, err
	}

	return &NeoDriver{
		pool: pool,
	}, nil
}

func (n *NeoDriver) Close(ctx context.Context) error {
	return n.pool.Close()
}

func (n *NeoDriver) ReadWithParams(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
	return n.read(query, params, mapp, single)
}

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
		msg := "error executing neo4j query"
		log.ErrorC(msg, err, nil)
		return errors.WithMessage(err, msg)
	}
	defer rows.Close()

	index := 0
	numOfResults := 0
results:
	for {

		log.Info("reading next row", log.Data{"numOfRes": numOfResults})
		data, meta, nextNeoErr := rows.NextNeo()
		if nextNeoErr != nil {
			if nextNeoErr != io.EOF {
				msg := "extractResults: rows.NextNeo() return unexpected error"
				log.ErrorC(msg, nextNeoErr, nil)
				return errors.WithMessage(nextNeoErr, msg)
			}
			break results
		}

		numOfResults++
		if single && index > 0 {
			msg := "non unique results"
			log.ErrorC(msg, err, nil)
			return errors.WithMessage(err, msg)
		}

		if mapp != nil {
			if err := mapp(&mapper.Result{Data: data, Meta: meta, Index: index}); err != nil {
				msg := "mapResult returned an error"
				log.ErrorC(msg, err, nil)
				return errors.WithMessage(err, msg)
			}
		}
		index++
	}

	if numOfResults == 0 {
		return driver.ErrNotFound
	}

	return nil
}

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
	// calling .CLose() which will ultimately release the connection back into the pool
	return NewBoltRowReader(rows), nil
}

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

func (n *NeoDriver) Exec(query string, params map[string]interface{}) (bolt.Result, error) {
	c, err := n.pool.OpenPool()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	return c.ExecNeo(query, params)
}
