package models

import (
	"errors"
	"time"
)

// AuditEvent represents an audit log entry for actions performed on a dataset or version
type AuditEvent struct {
	CreatedAt   time.Time   `bson:"created_at" json:"created_at"`
	RequestedBy RequestedBy `bson:"requested_by" json:"requested_by"`
	Action      Action      `bson:"action" json:"action"`
	Resource    string      `bson:"resource" json:"resource"`
	Dataset     *Dataset    `bson:"dataset,omitempty" json:"dataset,omitempty"`
	Version     *Version    `bson:"version,omitempty" json:"version,omitempty"`
}

// RequestedBy contains information about the user who initiated the action
type RequestedBy struct {
	ID    string `bson:"id" json:"id"`
	Email string `bson:"email,omitempty" json:"email,omitempty"`
}

// Action represents the type of action performed
type Action string

const (
	ActionCreate Action = "CREATE"
	ActionRead   Action = "READ"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

// NewAuditEvent creates a new AuditEvent instance
// It requires either a dataset or a version to be provided, but not both
func NewAuditEvent(requestedBy RequestedBy, action Action, resource string, dataset *Dataset, version *Version) (*AuditEvent, error) {
	if (dataset == nil) == (version == nil) {
		return nil, errors.New("either dataset or version must be provided, but not both")
	}

	return &AuditEvent{
		CreatedAt:   time.Now().UTC(),
		RequestedBy: requestedBy,
		Action:      action,
		Resource:    resource,
		Dataset:     dataset,
		Version:     version,
	}, nil
}
