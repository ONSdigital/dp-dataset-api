package cantabular

import (
	"context"
	"fmt"
	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
)

type adapter struct {
	client *cantabular.Client
}

func (c *adapter) Blobs(ctx context.Context) ([]models.Blob, error) {
	fmt.Printf("-------------------------> %+v", ctx)
	names, err := c.client.GetBlobs(ctx)
	if err != nil {
		return nil, err
	}
	blobs := make([]models.Blob, len(names))
	for i, name := range names {
		blobs[i] = models.NewBlob(name)
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

func NewCantabular(ctx context.Context, config config.CantabularConfig) *adapter {
	cantabularConfig := cantabular.Config{
		Host:           config.CantabularURL,
		ExtApiHost:     config.CantabularExtURL,
		GraphQLTimeout: config.DefaultRequestTimeout,
	}
	userAgent := dphttp.NewClient()
	return &adapter{
		client: cantabular.NewClient(cantabularConfig, userAgent, nil),
	}
}
