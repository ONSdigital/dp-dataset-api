package observation

import (
	"context"
	"errors"
	"io"
)

//go:generate moq -out observationtest/row_reader.go -pkg observationtest . StreamRowReader

// Check that the reader conforms to the io.reader interface.
var _ io.Reader = (*Reader)(nil)

// StreamRowReader provides a reader of individual rows (lines) of a CSV.
type StreamRowReader interface {
	Read() (string, error) // TODO: this should take context
	Close(context.Context) error
}

// ErrNoDataReturned is returned if no data was read.
var ErrNoDataReturned = errors.New("no data returned in this row")

// ErrUnrecognisedType is returned if a row does not have the expected string value.
var ErrUnrecognisedType = errors.New("the value returned was not a string")

// ErrNoInstanceFound is returned if no instance exists
var ErrNoInstanceFound = errors.New("no instance found in datastore")

// ErrNoResultsFound is returned if the selected filter options produce no results
var ErrNoResultsFound = errors.New("the filter options created no results")

// Reader is an io.Reader implementation that wraps a csvRowReader
type Reader struct {
	csvRowReader   StreamRowReader
	buffer         []byte // buffer a portion of the current line
	eof            bool   // are we at the end of the csv rows?
	totalBytesRead int64  // how many bytes in total have been read?
	obsCount       int32
}

// NewReader returns a new io.Reader for the given csvRowReader.
func NewReader(csvRowReader StreamRowReader) *Reader {
	return &Reader{
		csvRowReader: csvRowReader,
	}
}

// Read bytes from the underlying csvRowReader
func (reader *Reader) Read(p []byte) (n int, err error) {

	// check if the next line needs to be read.
	if reader.buffer == nil || len(reader.buffer) == 0 {
		csvRow, err := reader.csvRowReader.Read()
		if err == io.EOF {
			reader.eof = true
		} else if err != nil {
			return 0, err
		}

		reader.buffer = []byte(csvRow)
		reader.obsCount++
	}

	// copy into the given byte array.
	copied := copy(p, reader.buffer)
	reader.totalBytesRead += int64(copied)

	// if the line is bigger than the array, slice the line to account for bytes read
	if len(reader.buffer) > len(p) {
		reader.buffer = reader.buffer[copied:]
	} else { // the line is smaller than the array - clear the current line as it has all been read.
		reader.buffer = nil

		if reader.eof {
			return copied, io.EOF
		}
	}

	return copied, nil
}

// Close the reader.
func (reader *Reader) Close(ctx context.Context) (err error) {
	return reader.csvRowReader.Close(ctx)
}

// TotalBytesRead returns the total number of bytes read by this reader.
func (reader *Reader) TotalBytesRead() int64 {
	return reader.totalBytesRead
}

// ObservationsCount returns the total number of rows read by this reader.
func (reader *Reader) ObservationsCount() int32 {
	return reader.obsCount
}
