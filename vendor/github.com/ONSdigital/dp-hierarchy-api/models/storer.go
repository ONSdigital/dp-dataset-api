package models

import "context"

//go:generate moq -out modelstest/storer.go -pkg modelstest . Storer

// Storer is the generic interface for the database
type Storer interface {
	Close(ctx context.Context) error
	GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (string, error)
	GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (*Response, error)
	GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (*Response, error)
}
