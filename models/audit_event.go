package models

import (
	"errors"
	"time"
)

// AuditEvent represents an audit log entry for actions performed on a dataset, version, edition, or metadata
type AuditEvent struct {
	CreatedAt   time.Time   `bson:"created_at" json:"created_at"`
	RequestedBy RequestedBy `bson:"requested_by" json:"requested_by"`
	Action      Action      `bson:"action" json:"action"`
	Resource    string      `bson:"resource" json:"resource"`
	Dataset     *Dataset    `bson:"dataset,omitempty" json:"dataset,omitempty"`
	Version     *Version    `bson:"version,omitempty" json:"version,omitempty"`
	Edition     *Edition    `bson:"edition,omitempty" json:"edition,omitempty"`
	Metadata    *Metadata   `bson:"metadata,omitempty" json:"metadata,omitempty"`
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
// It requires exactly one of dataset, version, edition, or metadata to be provided
func NewAuditEvent(requestedBy RequestedBy, action Action, resource string, dataset *Dataset, version *Version, edition *Edition, metadata *Metadata) (*AuditEvent, error) {
	provided := 0
	if dataset != nil {
		provided++
	}
	if version != nil {
		provided++
	}
	if edition != nil {
		provided++
	}
	if metadata != nil {
		provided++
	}

	if provided != 1 {
		return nil, errors.New("exactly one of dataset, version, edition, or metadata must be provided")
	}

	return &AuditEvent{
		CreatedAt:   time.Now().UTC(),
		RequestedBy: requestedBy,
		Action:      action,
		Resource:    resource,
		Dataset:     dataset,
		Version:     version,
		Edition:     edition,
		Metadata:    metadata,
	}, nil
}
