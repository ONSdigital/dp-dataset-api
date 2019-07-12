package neptune

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/model"
)

func (n *NeptuneDB) InsertDimension(ctx context.Context, cache map[string]string, i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	return nil, nil
}
