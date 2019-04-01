package graph

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-graph/config"
	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/mock"
)

// DB contains the widest possible selection of functionality provided by any
// implementation of this abstraction
type DB struct {
	driver.Driver

	driver.CodeList
	driver.Hierarchy
	driver.Instance
	driver.Observation
	driver.Dimension
}

// Subsets allows a clear and concise way of requesting any combination of
// functionality by groups of node types
type Subsets struct {
	CodeList    bool
	Hierarchy   bool
	Instance    bool
	Observation bool
	Dimension   bool
}

// NewCodeListStore returns a configured DB containing the CodeList functionality
func NewCodeListStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{CodeList: true})
}

// NewHierarchyStore returns a configured DB containing the Hierarchy functionality
func NewHierarchyStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Hierarchy: true})
}

// NewObservationStore returns a configured DB containing the Observation functionality
func NewObservationStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Observation: true})
}

// NewInstanceStore returns a configured DB containing the Instance functionality
func NewInstanceStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Instance: true})
}

// NewDimensionStore returns a configured DB containing the Dimension functionality
func NewDimensionStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Dimension: true})
}

// New DB returned according to provided subsets and the environment config
// satisfying the interfaces requested by the choice of subsets
func New(ctx context.Context, choice Subsets) (*DB, error) {
	cfg, err := config.Get()
	if err != nil {
		return nil, err
	}

	var ok bool
	var codelist driver.CodeList
	if choice.CodeList {
		if codelist, ok = cfg.Driver.(driver.CodeList); !ok {
			return nil, errors.New("configured driver does not implement code list subset")
		}
	}

	var hierarchy driver.Hierarchy
	if choice.Hierarchy {
		if hierarchy, ok = cfg.Driver.(driver.Hierarchy); !ok {
			return nil, errors.New("configured driver does not implement hierarchy subset")
		}
	}

	var instance driver.Instance
	if choice.Instance {
		if instance, ok = cfg.Driver.(driver.Instance); !ok {
			return nil, errors.New("configured driver does not implement instance subset")
		}
	}

	var observation driver.Observation
	if choice.Observation {
		if observation, ok = cfg.Driver.(driver.Observation); !ok {
			return nil, errors.New("configured driver does not implement observation subset")
		}
	}

	var dimension driver.Dimension
	if choice.Dimension {
		if dimension, ok = cfg.Driver.(driver.Dimension); !ok {
			return nil, errors.New("configured driver does not implement dimension subset")
		}
	}

	return &DB{
		cfg.Driver,
		codelist,
		hierarchy,
		instance,
		observation,
		dimension,
	}, nil
}

// Test sets flags for managing responses from the Mock driver
func Test(backend, query, content bool) *mock.Mock {
	return &mock.Mock{
		IsBackendReachable: backend,
		IsQueryValid:       query,
		IsContentFound:     content,
	}
}
