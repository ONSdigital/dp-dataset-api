package models

import (
	"context"
)

type Blobs struct {
	Items []Blob `json:"items"`
}

func NewBlobs(ctx context.Context, cantabular CantabularDataProvider) (Blobs, error) {
	items, err := cantabular.Blobs(ctx)
	if err != nil {
		return Blobs{}, err
	}
	blobs := Blobs{items}
	return blobs, nil
}
