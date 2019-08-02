package gremgo

import (
	"context"
	"io"
	"net/http"

	"github.com/ONSdigital/graphson"
	"github.com/pkg/errors"
)

// Cursor allows for results to be iterated over as soon as available, rather than waiting for
// a query to complete and all results to be returned in one block.
type Cursor struct {
	ID string
}

// Stream is a specific implementation of a Cursor, which iterates over results from a cursor but
// only works on queries which return a list of strings. This is designed for returning what would
// be considered 'rows' of data in other contexts.
type Stream struct {
	cursor *Cursor
	eof    bool
	buffer []string
	client *Client
}

// Read a string response from the stream cursor, reading from the buffer of previously retrieved responses
// when possible. When the buffer is empty, Read uses the stream's client to retrieve further
// responses from the database.
func (s *Stream) Read() (string, error) {
	if len(s.buffer) == 0 {
		if s.eof {
			return "", io.EOF
		}

		if err := s.refillBuffer(); err != nil {
			return "", err
		}
	}

	var row string
	row, s.buffer = s.buffer[0], s.buffer[1:]
	row += "\n"

	return row, nil

}

func (s *Stream) refillBuffer() error {
	var resp []Response
	var err error

	for resp == nil && !s.eof { //resp could be empty if reading too quickly
		if resp, s.eof, err = s.client.retrieveNextResponseCtx(context.Background(), s.cursor); err != nil {
			return errors.Wrapf(err, "cursor.Read: %s", s.cursor.ID)
		}

		if len(resp) > 1 {
			return errors.New("too many results in cursor response")
		}

		//gremlin has returned a validly formed 'no content' response
		if len(resp) == 1 && &resp[0].Status != nil && resp[0].Status.Code == http.StatusNoContent {
			s.eof = true
			return io.EOF
		}
	}

	if s.buffer, err = graphson.DeserializeStringListFromBytes(resp[0].Result.Data); err != nil {
		return err
	}

	if len(s.buffer) == 0 {
		return errors.New("no results deserialized")
	}

	return nil
}

// Close satisfies the Closer interface. The stream does not need to close any
// resources, as the contained client holds the connection and is responsible
// for closing its own resources.
func (s *Stream) Close(ctx context.Context) error {
	return nil
}
