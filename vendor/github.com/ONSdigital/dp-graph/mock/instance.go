package mock

import (
	"context"
)

func (m *Mock) CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error) {
	return 0, m.checkForErrors()
}

func (m *Mock) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	return m.checkForErrors()
}

func (m *Mock) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	return m.checkForErrors()
}
