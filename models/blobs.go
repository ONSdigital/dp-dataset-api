package models

import (
	"context"
)

type Blobs struct {
	Items []Blob `json:"items"`
}

func NewBlobs(ctx context.Context, cantabular CantabularDataProvider) (Blobs, error) {
	return cantabular.Blobs(ctx)
}
