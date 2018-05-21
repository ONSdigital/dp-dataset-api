package observation

import (
	"io"

	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/errors"
)

//go:generate moq -out observationtest/bolt_rows.go -pkg observationtest . BoltRows
//go:generate moq -out observationtest/row_reader.go -pkg observationtest . CSVRowReader
//go:generate moq -out observationtest/db_connection.go -pkg observationtest . DBConnection

// BoltRows provides an interface to each row of results returned from the database.
type BoltRows bolt.Rows

// CSVRowReader provides a reader of individual rows (lines) of a CSV file.
type CSVRowReader interface {
	Read() (string, error)
	Close() error
}

// DBConnection provides a method to close the connection once all the rows have been read
type DBConnection interface {
	Close() error
}

// BoltRowReader translates Neo4j rows to CSV rows.
type BoltRowReader struct {
	rows       BoltRows
	connection DBConnection
	rowsRead   int
}

// NewBoltRowReader returns a new reader instace for the given bolt rows.
func NewBoltRowReader(rows BoltRows, connection DBConnection) *BoltRowReader {
	return &BoltRowReader{
		rows:       rows,
		connection: connection,
	}
}

// ErrNoDataReturned is returned if a Neo4j row has no data.
var ErrNoDataReturned = errors.New("no data returned in this row")

// ErrUnrecognisedType is returned if a Neo4j row does not have the expected string value.
var ErrUnrecognisedType = errors.New("the value returned was not a string")

// ErrNoInstanceFound is returned if no instance exists in neo4j
var ErrNoInstanceFound = errors.New("no instance found in datastore")

// ErrNoResultsFound is returned if the selected filter options produce no results
var ErrNoResultsFound = errors.New("the filter options created no results")

// Read the next row, or return io.EOF
func (reader *BoltRowReader) Read() (string, error) {
	data, _, err := reader.rows.NextNeo()
	if err != nil {
		if err == io.EOF {
			if reader.rowsRead == 0 {
				return "", ErrNoInstanceFound
			} else if reader.rowsRead == 1 {
				return "", ErrNoResultsFound
			}
		}
		return "", err
	}

	if len(data) < 1 {
		return "", ErrNoDataReturned
	}

	if csvRow, ok := data[0].(string); ok {
		reader.rowsRead++
		return csvRow + "\n", nil
	}

	return "", ErrUnrecognisedType
}

// Close the reader and the connection (For pooled connections this will release it back into the pool)
func (reader *BoltRowReader) Close() error {
	err := reader.rows.Close()
	if err != nil {
		return err
	}
	return reader.connection.Close()
}
