package neptune

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-graph/neptune/query"
)

// InsertDimension node to neptune and create relationships to the instance node.
// Where nodes and relationships already exist, ensure they are upserted.
func (n *NeptuneDB) InsertDimension(ctx context.Context, uniqueDimensions map[string]string, i *model.Instance, d *model.Dimension) (*model.Dimension, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}

	dimensionLabel := fmt.Sprintf("_%s_%s", i.InstanceID, d.DimensionID)

	res, err := n.getVertex(fmt.Sprintf(query.CreateDimensionToInstanceRelationship, i.InstanceID, d.DimensionID, d.Option, i.InstanceID, d.DimensionID, d.Option, i.InstanceID))
	if err != nil {
		return nil, err
	}

	d.NodeID = res.GetID()

	if _, ok := uniqueDimensions[dimensionLabel]; !ok {
		uniqueDimensions[dimensionLabel] = dimensionLabel
		i.AddDimension(d)
	}
	return d, nil
}
