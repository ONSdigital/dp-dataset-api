package neptune

import (
	"context"

	"github.com/ONSdigital/dp-dimension-importer/model"
)

func (n *NeptuneDB) CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error) {
	return 0, nil
}

func (n *NeptuneDB) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	return nil
}

func (n *NeptuneDB) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	return nil
}

func (n *NeptuneDB) CreateInstanceConstraint(ctx context.Context, i *model.Instance) error {
	return nil
}

func (n *NeptuneDB) CreateInstance(ctx context.Context, i *model.Instance) error {
	return nil
}

func (n *NeptuneDB) AddDimensions(ctx context.Context, i *model.Instance) error {
	return nil
}

func (n *NeptuneDB) CreateCodeRelationship(ctx context.Context, i *model.Instance, codeListID, code string) error {
	return nil
}

func (n *NeptuneDB) InstanceExists(ctx context.Context, i *model.Instance) (bool, error) {
	return true, nil
}
