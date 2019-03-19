package graph

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-graph/config"
	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/mock"
	"github.com/davecgh/go-spew/spew"
)

type DB struct {
	driver.Driver

	driver.CodeList
	driver.Hierarchy
	driver.Instance
	driver.Observation
}

type Subsets struct {
	CodeList    bool
	Hierarchy   bool
	Instance    bool
	Observation bool
}

func NewCodeListStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{CodeList: true})
}

func NewHierarchyStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Hierarchy: true})
}

func NewObservationStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Observation: true})
}

func NewInstanceStore(ctx context.Context) (*DB, error) {
	return New(ctx, Subsets{Instance: true})
}

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
			spew.Dump(cfg.Driver)
			return nil, errors.New("configured driver does not implement instance subset")
		}
	}

	var observation driver.Observation
	if choice.Observation {
		if observation, ok = cfg.Driver.(driver.Observation); !ok {
			return nil, errors.New("configured driver does not implement observation subset")
		}
	}

	return &DB{
		cfg.Driver,
		codelist,
		hierarchy,
		instance,
		observation,
	}, nil
}

//Test sets flags for managing responses from the Mock driver
func Test(backend, query, content bool) *mock.Mock {
	return &mock.Mock{
		IsBackendReachable: backend,
		IsQueryValid:       query,
		IsContentFound:     content,
	}
}
