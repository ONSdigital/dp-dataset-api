package application

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
)

// recordAuditEvent validates and records an audit event for either a dataset or version.
// It is an internal function used by RecordDatasetAuditEvent and RecordVersionAuditEvent
func (smDS *StateMachineDatasetAPI) recordAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset, version *models.Version) error {
	event, err := models.NewAuditEvent(requestedBy, action, resource, dataset, version)
	if err != nil {
		return fmt.Errorf("recordAuditEvent: failed to create audit event model: %w", err)
	}

	if err := smDS.DataStore.Backend.CreateAuditEvent(ctx, event); err != nil {
		return fmt.Errorf("recordAuditEvent: failed to create audit event in store: %w", err)
	}

	return nil
}

// RecordDatasetAuditEvent records an audit event for a dataset action
func (smDS *StateMachineDatasetAPI) RecordDatasetAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
	return smDS.recordAuditEvent(ctx, requestedBy, action, resource, dataset, nil)
}

// RecordVersionAuditEvent records an audit event for a version action
func (smDS *StateMachineDatasetAPI) RecordVersionAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
	return smDS.recordAuditEvent(ctx, requestedBy, action, resource, nil, version)
}
