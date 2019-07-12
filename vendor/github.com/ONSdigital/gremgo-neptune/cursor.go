package gremgo

import (
	"context"
	"io"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/gedge/graphson"
	"github.com/pkg/errors"
)

// Cursor allows for results to be iterated over as soon as available, rather than waiting for
// a query to complete and all results to be returned in one block.
type Cursor struct {
	ID     string
	mu     sync.RWMutex
	eof    bool
	buffer []string
	client *Client
}

// Read a string response from the cursor, reading from the buffer of previously retrieved responses
// when possible. When the buffer is empty, Read uses the cursor's client to retrieve further
// responses from the database. As this function does not take context, a number of attempts
// is hardcoded in refillBuffer() to prevent an infinite wait for further responses.
func (c *Cursor) Read() (string, error) {
	if len(c.buffer) == 0 {
		if c.eof {
			return "", io.EOF
		}

		if err := c.refillBuffer(); err != nil {
			return "", err
		}
	}

	s := c.buffer[0] + "\n"
	spew.Dump("cursor string: " + s)

	if len(c.buffer) > 1 {
		c.buffer = c.buffer[1:]
	} else {
		c.buffer = []string{}
	}

	return s, nil

}

func (c *Cursor) refillBuffer() error {
	var resp []Response
	var err error

	var attempts int
	for resp == nil && !c.eof || attempts > 5 { //resp could be empty if reading too quickly
		attempts++
		if resp, c.eof, err = c.client.retrieveNextResponseCtx(context.Background(), c); err != nil {
			err = errors.Wrapf(err, "cursor.Read: %s", c.ID)
			return err
		}
	}

	//gremlin has returned a validly formed 'no content' response
	if len(resp) == 1 && &resp[0].Status != nil && resp[0].Status.Code == 204 {
		return io.ErrUnexpectedEOF
	}

	if c.buffer, err = graphson.DeserializeStringListFromBytes(resp[0].Result.Data); err != nil {
		return err
	}

	if len(c.buffer) == 0 {
		return errors.New("no results deserialized")
	}

	return nil
}

// Close satisfies the ReadCloser interface. The cursor does not need to close any
// resources, as the contained client holds the connection, and this is closed
// by the defered close in OpenCursorCtx
func (c *Cursor) Close(ctx context.Context) error {
	return nil
}
