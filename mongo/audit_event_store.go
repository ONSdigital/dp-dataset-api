package mongo

import (
	"context"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
)

// CreateAuditEvent inserts a new audit event into the dataset_events collection
func (m *Mongo) CreateAuditEvent(ctx context.Context, event *models.AuditEvent) error {
	_, err := m.Connection.Collection(m.ActualCollectionName(config.DatasetEventsCollection)).InsertOne(ctx, event)
	return err
}
