package driver

import (
	"context"
	"errors"

	codelistModels "github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/dp-graph/observation"
	hierarchyModels "github.com/ONSdigital/dp-hierarchy-api/models"
)

var ErrNotFound = errors.New("not found")

type Driver interface {
	Close(ctx context.Context) error
	Healthcheck() (string, error)
}

type CodeList interface {
	GetCodeLists(ctx context.Context, filterBy string) (*codelistModels.CodeListResults, error)
	GetCodeList(ctx context.Context, codeListID string) (*codelistModels.CodeList, error)
	GetEditions(ctx context.Context, codeListID string) (*codelistModels.Editions, error)
	GetEdition(ctx context.Context, codeListID, edition string) (*codelistModels.Edition, error)
	GetCodes(ctx context.Context, codeListID, edition string) (*codelistModels.CodeResults, error)
	GetCode(ctx context.Context, codeListID, edition string, code string) (*codelistModels.Code, error)
	GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*codelistModels.Datasets, error)
}

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

// Observation provides filtered observation data in CSV rows.
type Observation interface {
	StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error)
}

type Instance interface {
	//CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error)
	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
}
