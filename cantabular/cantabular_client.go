package cantabular

import "context"

//go:generate moq -out mock/cantabular_client.go -pkg mock . CantabularClient

// CantabularClient fetches lists of datasets
type CantabularClient interface {
	ListDatasets(ctx context.Context) ([]string, error)
}
