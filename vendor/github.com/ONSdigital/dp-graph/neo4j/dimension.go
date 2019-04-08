package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/neo4j/query"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
)

// InsertDimesion node to neo4j and create a unique constraint on the dimension
// label & value if one does not already exist.
func (n *Neo4j) InsertDimension(ctx context.Context, cache map[string]string, i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}

	dimensionLabel := fmt.Sprintf("_%s_%s", i.InstanceID, d.DimensionID)

	if _, exists := cache[dimensionLabel]; !exists {

		if err := n.createUniqueConstraint(ctx, i.InstanceID, d); err != nil {
			return nil, err
		}
		cache[dimensionLabel] = dimensionLabel
		i.AddDimension(d)
	}

	if err := n.insertDimension(ctx, i, d); err != nil {
		return nil, err
	}
	return d, nil
}

func (n *Neo4j) createUniqueConstraint(ctx context.Context, instanceID string, d *model.Dimension) error {
	stmt := fmt.Sprintf(query.CreateDimensionConstraint, instanceID, d.DimensionID)

	if _, err := n.Exec(stmt, nil); err != nil {
		return errors.Wrap(err, "neoClient.Exec returned an error")
	}

	log.Info("successfully created unique constraint on dimension", log.Data{"dimension": d.DimensionID})
	return nil
}

func (n *Neo4j) insertDimension(ctx context.Context, i *model.Instance, d *model.Dimension) error {
	logData := log.Data{
		"dimension_id": d.DimensionID,
		"value":        d.Option,
	}

	var err error
	params := map[string]interface{}{"value": d.Option}
	logData["params"] = params

	stmt := fmt.Sprintf(query.CreateDimensionToInstanceRelationship, i.InstanceID, i.InstanceID, d.DimensionID)
	logData["statement"] = stmt

	nodeID := new(string)
	if err = n.ReadWithParams(stmt, params, mapper.GetNodeID(nodeID), true); err != nil {
		return errors.Wrap(err, "neoClient.ReadWithParams returned an error")
	}

	d.NodeID = *nodeID
	return nil
}
