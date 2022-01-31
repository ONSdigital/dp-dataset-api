package cantabular

import (
	"context"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

//go:generate moq -out mock/cantabular_client.go -pkg mock . CantabularClient

// CantabularClient fetches lists of datasets
type CantabularClient interface {
	ListDatasets(ctx context.Context) ([]string, error)
	Checker(ctx context.Context, state *healthcheck.CheckState) error
}
