package driver

import (
	"context"

	codelistModels "github.com/ONSdigital/dp-code-list-api/models"
	importModels "github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-graph/observation"
	hierarchyModels "github.com/ONSdigital/dp-hierarchy-api/models"
	obsModels "github.com/ONSdigital/dp-observation-importer/models"
)

// Driver is the base interface any driver implementation must satisfy
type Driver interface {
	Close(ctx context.Context) error
	Healthcheck() (string, error)
}

// Codelist defines functions to retrieve code list and code nodes
type CodeList interface {
	GetCodeLists(ctx context.Context, filterBy string) (*codelistModels.CodeListResults, error)
	GetCodeList(ctx context.Context, codeListID string) (*codelistModels.CodeList, error)
	GetEditions(ctx context.Context, codeListID string) (*codelistModels.Editions, error)
	GetEdition(ctx context.Context, codeListID, edition string) (*codelistModels.Edition, error)
	GetCodes(ctx context.Context, codeListID, edition string) (*codelistModels.CodeResults, error)
	GetCode(ctx context.Context, codeListID, edition string, code string) (*codelistModels.Code, error)
	GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*codelistModels.Datasets, error)
}

// Hierarchy defines functions to create and retrieve generic and instance hierarchy nodes
type Hierarchy interface {
	CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error
	CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error
	CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error)
	CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error
	SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) error
	SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) error
	MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error
	RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error
	RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) error

	GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (string, error)
	GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (*hierarchyModels.Response, error)
	GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (*hierarchyModels.Response, error)
}

// Observation defines functions to create and retrieve observation nodes
type Observation interface {
	// StreamCSVRows returns a reader which the caller is ultimately responsible for closing
	// This allows for large volumes of data to be read from a stream without signnificant
	// memory overhead.
	StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error)
	InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*obsModels.Observation, dimensionIDs map[string]string) error
}

// Instance defines functions to create, update and retrieve details about instances
type Instance interface {
	CreateInstanceConstraint(ctx context.Context, i *importModels.Instance) error
	CreateInstance(ctx context.Context, i *importModels.Instance) error
	AddDimensions(ctx context.Context, i *importModels.Instance) error
	CreateCodeRelationship(ctx context.Context, i *importModels.Instance, codeListID, code string) error
	InstanceExists(ctx context.Context, i *importModels.Instance) (bool, error)
	CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error)
	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
}

// Dimension defines functions to create dimension nodes
type Dimension interface {
	InsertDimension(ctx context.Context, cache map[string]string, i *importModels.Instance, d *importModels.Dimension) (*importModels.Dimension, error)
}
