package cantabular

import (
	"context"
	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
)

//go:generate moq -out mocks/client.go -pkg mocks . Client

// Client is the (private) outward facing side of the adapter which sits inside the adapter struct. It should not be used directly and is only exposed to allow for testing
type Client interface {
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	CheckerAPIExt(ctx context.Context, state *healthcheck.CheckState) error
	GetPopulationTypes(ctx context.Context) ([]string, error)
}

// adapter implements the inward facing (public) side of the adapter
type adapter struct {
	client Client
}

func (c *adapter) PopulationTypes(ctx context.Context) ([]models.PopulationType, error) {
	names, err := c.client.GetPopulationTypes(ctx)
	if err != nil {
		return nil, err
	}
	blobs := make([]models.PopulationType, len(names))
	for i, name := range names {
		blobs[i] = models.NewPopulationType(name)
	}
	return blobs, nil
}

func (c *adapter) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if err := c.client.Checker(ctx, state); err != nil {
		return err
	}
	if err := c.client.CheckerAPIExt(ctx, state); err != nil {
		return err
	}
	return nil
}

func NewCantabularAdapter(config config.CantabularConfig) *adapter {
	return NewCantabularAdapterForStrategy(config, buildRuntimeClient)
}

func buildRuntimeClient(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) Client {
	return cantabular.NewClient(cantabularConfig, userAgent, nil)
}

// NewCantabularAdapterForStrategy is a test seam and shouldn't be used directly
func NewCantabularAdapterForStrategy(config config.CantabularConfig, buildStrategy AdapterStrategy) *adapter {

	cantabularConfig := cantabular.Config{
		Host:           config.CantabularURL,
		ExtApiHost:     config.CantabularExtURL,
		GraphQLTimeout: config.DefaultRequestTimeout,
	}
	userAgent := dphttp.NewClient()
	client := buildStrategy(cantabularConfig, userAgent)
	return &adapter{
		client,
	}
}

type AdapterStrategy func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) Client
