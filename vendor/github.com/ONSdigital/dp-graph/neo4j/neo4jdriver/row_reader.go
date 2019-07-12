package neo4jdriver

import (
	"context"
	"io"

	"github.com/ONSdigital/dp-graph/observation"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
)

//go:generate moq -out ../internal/bolt_rows.go -pkg internal . BoltRows
//go:generate moq -out ../internal/bolt_conn.go -pkg internal . BoltConn

type BoltRows bolt.Rows
type BoltConn bolt.Conn

// BoltRowReader translates Neo4j rows to CSV rows.
type BoltRowReader struct {
	rows       bolt.Rows
	connection bolt.Conn
	rowsRead   int
}

// NewBoltRowReader returns a new reader instace for the given bolt rows.
func NewBoltRowReader(rows bolt.Rows, conn bolt.Conn) *BoltRowReader {
	return &BoltRowReader{
		rows:       rows,
		connection: conn,
	}
}

// Close the contained rows and database connection
func (reader *BoltRowReader) Close(ctx context.Context) error {
	defer reader.connection.Close()
	return reader.rows.Close()
}

// Read the next row, or return io.EOF
func (reader *BoltRowReader) Read() (string, error) {
	data, _, err := reader.rows.NextNeo()
	if err != nil {
		if err == io.EOF {
			if reader.rowsRead == 0 {
				return "", observation.ErrNoInstanceFound
			} else if reader.rowsRead == 1 {
				return "", observation.ErrNoResultsFound
			}
		}
		return "", err
	}

	if len(data) < 1 {
		return "", observation.ErrNoDataReturned
	}

	if csvRow, ok := data[0].(string); ok {
		reader.rowsRead++
		return csvRow + "\n", nil
	}

	return "", observation.ErrUnrecognisedType
}
