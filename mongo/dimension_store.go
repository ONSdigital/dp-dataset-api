package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

const maxIDs = 1000

// GetDimensionsFromInstance returns a list of dimensions and their options for an instance resource.
// Note that all dimension options for all dimensions are returned as high level items, hence there can be duplicate dimension names,
// which correspond to different options.
func (m *Mongo) GetDimensionsFromInstance(ctx context.Context, id string, offset, limit int) ([]*models.DimensionOption, int, error) {
	dimensions := []*models.DimensionOption{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Find(ctx, bson.M{"instance_id": id}, &dimensions,
		mongodriver.Projection(bson.M{"id": 0, "last_updated": 0, "instance_id": 0}),
		mongodriver.Offset(offset),
		mongodriver.Limit(limit))
	if err != nil {
		return dimensions, 0, err
	}

	return dimensions, totalCount, nil
}

// GetUniqueDimensionAndOptions returns a list of dimension options for an instance resource
func (m *Mongo) GetUniqueDimensionAndOptions(ctx context.Context, id, dimension string) (uniqueValues []*string, countOfValues int, err error) {
	vals, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Distinct(ctx, "option", bson.M{"instance_id": id, "name": dimension})
	if err != nil {
		return nil, 0, err
	}

	if len(vals) == 0 {
		return nil, 0, errs.ErrDimensionNodeNotFound
	}

	values := []*string{}
	for _, v := range vals {
		if vs, ok := v.(string); ok {
			values = append(values, &vs)
		}
	}

	return values, len(values), nil
}

// UpsertDimensionsToInstance to the dimension collection
func (m *Mongo) UpsertDimensionsToInstance(ctx context.Context, opts []*models.CachedDimensionOption) error {
	now := time.Now().UTC()
	for _, opt := range opts {
		option := models.DimensionOption{InstanceID: opt.InstanceID, Option: opt.Option, Name: opt.Name, Label: opt.Label}
		option.Order = opt.Order
		option.Links.CodeList = models.LinkObject{ID: opt.CodeList, HRef: fmt.Sprintf("%s/code-lists/%s", m.MongoConfig.CodeListAPIURL, opt.CodeList)}
		option.Links.Code = models.LinkObject{ID: opt.Code, HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", m.MongoConfig.CodeListAPIURL, opt.CodeList, opt.Code)}
		option.LastUpdated = now
		if _, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Upsert(ctx,
			bson.M{"instance_id": option.InstanceID, "name": option.Name, "option": option.Option},
			bson.M{"$set": option}); err != nil {
			return err
		}
	}

	return nil
}

// GetDimensions returns a list of all dimensions from a dataset
func (m *Mongo) GetDimensions(ctx context.Context, versionID string) ([]bson.M, error) {
	// To get all unique values an aggregation is needed, as using distinct() will only return the distinct values and
	// not the documents.
	// Match by instance_id
	match := bson.M{"$match": bson.M{"instance_id": versionID}}
	// Then group the values by name.
	group := bson.M{"$group": bson.M{"_id": "$name", "doc": bson.M{"$first": "$$ROOT"}}}
	results := []bson.M{}
	err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Aggregate(ctx, []bson.M{match, group}, &results)
	if err != nil {
		return nil, err
	}

	if len(results) < 1 {
		return nil, errs.ErrDimensionsNotFound
	}

	return results, nil
}

// GetDimensionOptions returns dimension options for a dimensions within a dataset, according to the provided limit and offset.
// Offset and limit need to be positive or zero
func (m *Mongo) GetDimensionOptions(ctx context.Context, version *models.Version, dimension string, offset, limit int) ([]*models.PublicDimensionOption, int, error) {
	// define selector to obtain all the dimension options for an instance
	selector := bson.M{"instance_id": version.ID, "name": dimension}

	s, err := m.sortOrder(ctx, selector)
	if err != nil {
		return nil, 0, err
	}

	// get total count and paginated values according to provided offset and limit
	values := []*models.PublicDimensionOption{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Find(ctx, selector, &values,
		s, mongodriver.Offset(offset), mongodriver.Limit(limit))
	if err != nil {
		return values, 0, err
	}

	// update links for returned values
	for i := 0; i < len(values); i++ {
		values[i].Links.Version = *version.Links.Self
	}

	return values, totalCount, nil
}

// GetDimensionOptionsFromIDs returns dimension options for a dimension within a dataset, whose IDs match the provided list of IDs
func (m *Mongo) GetDimensionOptionsFromIDs(ctx context.Context, version *models.Version, dimension string, ids []string) ([]*models.PublicDimensionOption, int, error) {
	if len(ids) > maxIDs {
		return nil, 0, errors.New("too many IDs provided")
	}

	selectorAll := bson.M{"instance_id": version.ID, "name": dimension}
	selectorInList := bson.M{"instance_id": version.ID, "name": dimension, "option": bson.M{"$in": ids}}

	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Count(ctx, selectorAll)
	if err != nil {
		return nil, 0, err
	}

	var values []*models.PublicDimensionOption
	if totalCount > 0 {
		// obtain query defining the order for the provided IDs only
		s, err := m.sortOrder(ctx, selectorInList)
		if err != nil {
			return nil, 0, err
		}

		// obtain all required options in order
		if _, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Find(ctx, selectorInList, &values,
			s, mongodriver.Limit(totalCount)); err != nil {
			return nil, 0, err
		}

		// update links for returned values
		for i := 0; i < len(values); i++ {
			values[i].Links.Version = *version.Links.Self
		}
	}

	return values, totalCount, nil
}

// UpdateDimensionsNodeIDAndOrder to cache the id and order (optional) for other import processes
func (m *Mongo) UpdateDimensionsNodeIDAndOrder(ctx context.Context, dimensions []*models.DimensionOption) error {
	// validate that there is something to update
	isUpdate := false
	for _, dim := range dimensions {
		if dim.Order != nil || dim.NodeID != "" {
			isUpdate = true
			break
		}
	}
	if !isUpdate {
		return nil // nothing to update
	}

	// queue all options to be updated
	now := time.Now().UTC()
	for _, dimension := range dimensions {
		update := bson.M{"last_updated": now}
		if dimension.NodeID != "" {
			update["node_id"] = &dimension.NodeID
		}
		if dimension.Order != nil {
			update["order"] = &dimension.Order
		}
		result, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Update(ctx,
			bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name, "option": dimension.Option},
			bson.M{"$set": update})
		if err != nil {
			return fmt.Errorf("error trying to update: %w", err)
		}
		if result.MatchedCount == 0 {
			log.Event(ctx, "failed to update dimension.options ", log.WARN, log.Data{"instance_id": dimension.InstanceID, "name": dimension.Name, "option": dimension.Option})
		}
	}

	return nil
}

// sortOrder generates a sort order from the provided bson.M selector
// if the order property exists, it will be used to determine the order
// otherwise, the items will be sorted alphabetically by option
func (m *Mongo) sortOrder(ctx context.Context, selector bson.M) (mongodriver.FindOption, error) {
	selector["order"] = bson.M{"$exists": true}
	orderCount, err := m.Connection.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)).Count(ctx, selector)
	if err != nil {
		return nil, err
	}
	delete(selector, "order")

	if orderCount > 0 {
		return mongodriver.Sort(bson.M{"order": 1}), nil
	}

	return mongodriver.Sort(bson.M{"option": 1}), nil
}
