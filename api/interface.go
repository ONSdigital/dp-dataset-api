package api

import (
	"context"
)

//go:generate moq -out mock/cantabular_client.go -pkg mock . CantabularClient
//go:generate moq -out mock/logger.go -pkg mock . Logger

// CantabularClient fetches lists of datasets
type CantabularClient interface {
	ListDatasets(ctx context.Context) ([]string, error)
}

type Logger interface {
	// Error wraps the Event function with the severity level set to ERROR
	Error(ctx context.Context, event string, err error)
}
