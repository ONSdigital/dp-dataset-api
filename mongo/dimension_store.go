package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

const maxIDs = 1000

// GetDimensionsFromInstance returns a list of dimensions and their options for an instance resource.
// Note that all dimension options for all dimensions are returned as high level items, hence there can be duplicate dimension names,
// which correspond to different options.
func (m *Mongo) GetDimensionsFromInstance(ctx context.Context, id string, offset, limit int) ([]*models.DimensionOption, int, error) {

	f := m.Connection.C(dimensionOptions).Find(bson.M{"instance_id": id}).Select(bson.M{"id": 0, "last_updated": 0, "instance_id": 0})

	// get total count and paginated values according to provided offset and limit
	dimensions := []*models.DimensionOption{}
	totalCount, err := QueryPage(ctx, f, offset, limit, &dimensions)
	if err != nil {
		return dimensions, 0, err
	}

	return dimensions, totalCount, nil
}

// GetUniqueDimensionAndOptions returns a list of dimension options for an instance resource
func (m *Mongo) GetUniqueDimensionAndOptions(ctx context.Context, id, dimension string) ([]*string, int, error) {

	f, err := m.sortedQuery(ctx, bson.M{"instance_id": id, "name": dimension})
	if err != nil {
		return nil, 0, err
	}

	// Is Distinct necessary?
	// If we can guarantee that all options will be different for a dimension and instance
	// then we could create an iterator and get only the necessary items from the DB
	vals, err := f.Distinct(ctx, "option")
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

	// TODO At present there is no means to access the BulkWrite method on the golang Mongo diver's Collection, as our wrapping
	// of same in dp-mongodb does not expose the underlying collection.
	// I am leaving this todo-comment here as I will look at adding an accessor, so that the following commented code may be used
	// to do a bulk write in a similar fashion to what existed with the old Mongo globalsign driver
	// If I forget to remove this todo, please feel free to delete this comment and the underlying commented code after 31/12/21
	//
	//var upserts []mongoRawGoDriver.WriteModel
	//now := time.Now().UTC()
	//for _, opt := range opts {
	//	option := models.DimensionOption{InstanceID: opt.InstanceID, Option: opt.Option, Name: opt.Name, Label: opt.Label}
	//	option.Order = opt.Order
	//	option.Links.CodeList = models.LinkObject{ID: opt.CodeList, HRef: fmt.Sprintf("%s/code-lists/%s", m.MongoConfig.CodeListAPIURL, opt.CodeList)}
	//	option.Links.Code = models.LinkObject{ID: opt.Code, HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", m.MongoConfig.CodeListAPIURL, opt.CodeList, opt.Code)}
	//	option.LastUpdated = now
	//	upserts = append(upserts, mongoRawGoDriver.NewUpdateOneModel().
	//		SetFilter(bson.M{"instance_id": option.InstanceID, "name": option.Name, "option": option.Option}).
	//		SetUpdate(option).
	//		SetUpsert(true))
	//}
	//
	//_, err := m.Connection.C(dimensionOptions).GetMongoCollection().BulkWrite(ctx, upserts)
	//return err

	now := time.Now().UTC()
	for _, opt := range opts {
		option := models.DimensionOption{InstanceID: opt.InstanceID, Option: opt.Option, Name: opt.Name, Label: opt.Label}
		option.Order = opt.Order
		option.Links.CodeList = models.LinkObject{ID: opt.CodeList, HRef: fmt.Sprintf("%s/code-lists/%s", m.MongoConfig.CodeListAPIURL, opt.CodeList)}
		option.Links.Code = models.LinkObject{ID: opt.Code, HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", m.MongoConfig.CodeListAPIURL, opt.CodeList, opt.Code)}
		option.LastUpdated = now
		if _, err := m.Connection.C(dimensionOptions).Upsert(ctx,
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
	err := m.Connection.C(dimensionOptions).Aggregate([]bson.M{match, group}).All(ctx, &results)
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

	// obtain query defining the order
	f, err := m.sortedQuery(ctx, selector)
	if err != nil {
		return nil, 0, err
	}

	// get total count and paginated values according to provided offset and limit
	values := []*models.PublicDimensionOption{}
	totalCount, err := QueryPage(ctx, f, offset, limit, &values)
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
func (m *Mongo) GetDimensionOptionsFromIDs(ctx context.Context, version *models.Version, dimension string, IDs []string) ([]*models.PublicDimensionOption, int, error) {

	if len(IDs) > maxIDs {
		return nil, 0, errors.New("too many IDs provided")
	}

	selectorAll := bson.M{"instance_id": version.ID, "name": dimension}
	selectorInList := bson.M{"instance_id": version.ID, "name": dimension, "option": bson.M{"$in": IDs}}

	// count total number of options in dimension
	totalCount, err := m.Connection.C(dimensionOptions).Find(selectorAll).Count(ctx)
	if err != nil {
		return nil, 0, err
	}

	var values []*models.PublicDimensionOption
	if totalCount > 0 {
		// obtain query defining the order for the provided IDs only
		f, err := m.sortedQuery(ctx, selectorInList)
		if err != nil {
			return nil, 0, err
		}

		// obtain all required options in order
		if err = f.IterAll(ctx, &values); err != nil {
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

	// TODO As with UpsertDimensionsToInstance above, at present there is no means to access the BulkWrite method on the
	// golang Mongo diver's Collection, as our wrapping of same in dp-mongodb does not expose the underlying collection.
	// I am leaving this todo-comment here as I will look at adding an accessor, so that the following commented code may be used
	// to do a bulk write in a similar fashion to what existed with the old Mongo globalsign driver
	// If I forget to remove this todo, please feel free to delete this comment and the underlying commented code after 31/12/21
	//
	//var updates []mongoRawGoDriver.WriteModel
	//now := time.Now().UTC()
	//for _, dimension := range dimensions {
	//	update := bson.M{"last_updated": now}
	//	if dimension.NodeID != "" {
	//		update["node_id"] = &dimension.NodeID
	//	}
	//	if dimension.Order != nil {
	//		update["order"] = &dimension.Order
	//	}
	//	updates = append(updates, mongoRawGoDriver.NewUpdateOneModel().
	//		SetFilter(bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name, "option": dimension.Option}).
	//		SetUpdate(bson.M{"$set": update}))
	//}
	//
	//results, err := m.Connection.C(dimensionOptions).GetMongoCollection().BulkWrite(ctx, updates)
	//if err != nil {
	//	return err
	//}
	//if int(results.ModifiedCount) != len(dimensions) {
	//	return errs.ErrDimensionOptionNotFound
	//}
	//return nil

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
		if _, err := m.Connection.C(dimensionOptions).Must().Update(ctx,
			bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name, "option": dimension.Option},
			bson.M{"$set": update}); err != nil {
			return fmt.Errorf("error trying to update: %w", err)
		}
	}

	return nil
}

// sortedQuery generates a sorted mongoDB query from the provided bson.M selector
// if order property exists, it will be used to determine the order
// otherwise, the items will be sorted alphabetically by option
func (m *Mongo) sortedQuery(ctx context.Context, selector bson.M) (*mongodriver.Find, error) {

	selector["order"] = bson.M{"$exists": true}
	orderCount, err := m.Connection.C(dimensionOptions).Find(selector).Count(ctx)
	if err != nil {
		return nil, err
	}
	delete(selector, "order")

	f := m.Connection.C(dimensionOptions).Find(selector)
	if orderCount > 0 {
		return f.Sort(bson.M{"order": 1}), nil
	}

	return f.Sort(bson.M{"option": 1}), nil
}
