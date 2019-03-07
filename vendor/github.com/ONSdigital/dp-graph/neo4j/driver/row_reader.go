package driver

import (
	"context"
	"io"

	"github.com/ONSdigital/dp-graph/observation"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
)

//go:generate moq -out ../internal/bolt_rows.go -pkg internal . BoltRows

type BoltRows bolt.Rows

// BoltRowReader translates Neo4j rows to CSV rows.
type BoltRowReader struct {
	rows     bolt.Rows
	rowsRead int
}

// NewBoltRowReader returns a new reader instace for the given bolt rows.
func NewBoltRowReader(rows bolt.Rows) *BoltRowReader {
	return &BoltRowReader{
		rows: rows,
	}
}

func (reader *BoltRowReader) Close(ctx context.Context) error {
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
