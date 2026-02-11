package application

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

// AuditService defines the interface for audit logging
//
//go:generate moq -out mock/audit_service.go -pkg mock . AuditService
type AuditService interface {
	RecordDatasetAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error
	RecordVersionAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error
	RecordEditionAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, edition *models.Edition) error
	RecordMetadataAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, metadata *models.Metadata) error
}

// auditService provides methods for audit logging
type auditService struct {
	DataStore store.DataStore
}

// NewAuditService creates a new instance of AuditService
func NewAuditService(dataStore store.DataStore) AuditService {
	return &auditService{
		DataStore: dataStore,
	}
}

// recordAuditEvent validates and records an audit event for dataset, version, edition, or metadata
func (a *auditService) recordAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset, version *models.Version, edition *models.Edition, metadata *models.Metadata) error {
	event, err := models.NewAuditEvent(requestedBy, action, resource, dataset, version, edition, metadata)
	if err != nil {
		return fmt.Errorf("recordAuditEvent: failed to create audit event model: %w", err)
	}

	if err := a.DataStore.Backend.CreateAuditEvent(ctx, event); err != nil {
		return fmt.Errorf("recordAuditEvent: failed to create audit event in store: %w", err)
	}

	return nil
}

// RecordDatasetAuditEvent records an audit event for a dataset action
func (a *auditService) RecordDatasetAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
	return a.recordAuditEvent(ctx, requestedBy, action, resource, dataset, nil, nil, nil)
}

// RecordVersionAuditEvent records an audit event for a version action
func (a *auditService) RecordVersionAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
	return a.recordAuditEvent(ctx, requestedBy, action, resource, nil, version, nil, nil)
}

// RecordEditionAuditEvent records an audit event for an edition action
func (a *auditService) RecordEditionAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, edition *models.Edition) error {
	return a.recordAuditEvent(ctx, requestedBy, action, resource, nil, nil, edition, nil)
}

// RecordMetadataAuditEvent records an audit event for a metadata action
func (a *auditService) RecordMetadataAuditEvent(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, metadata *models.Metadata) error {
	return a.recordAuditEvent(ctx, requestedBy, action, resource, nil, nil, nil, metadata)
}
