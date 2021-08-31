package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const maxIDs = 1000

// GetDimensionsFromInstance returns a list of dimensions and their options for an instance resource.
// Note that all dimension options for all dimensions are returned as high level items, hence there can be duplicate dimension names,
// which correspond to different options.
func (m *Mongo) GetDimensionsFromInstance(ctx context.Context, id string, offset, limit int) ([]*models.DimensionOption, int, error) {
	s := m.Session.Copy()
	defer s.Close()

	q := s.DB(m.Database).C(dimensionOptions).
		Find(bson.M{"instance_id": id}).
		Select(bson.M{"id": 0, "last_updated": 0, "instance_id": 0})

	// get total count and paginated values according to provided offset and limit
	dimensions := []*models.DimensionOption{}
	totalCount, err := QueryPage(ctx, q, offset, limit, &dimensions)
	if err != nil {
		return dimensions, 0, err
	}

	return dimensions, totalCount, nil
}

// GetUniqueDimensionAndOptions returns a list of dimension options for an instance resource
func (m *Mongo) GetUniqueDimensionAndOptions(ctx context.Context, id, dimension string, offset, limit int) ([]*string, int, error) {
	s := m.Session.Copy()
	defer s.Close()

	q, err := m.sortedQuery(s, bson.M{"instance_id": id, "name": dimension})
	if err != nil {
		return nil, 0, err
	}

	// Is Disctinct necessary?
	// If we can guarantee that all options will be different for a dimension and instance
	// then we could create an iterator and get only the necessary items from the DB
	values := []*string{}
	err = q.Distinct("option", &values)
	if err != nil {
		return nil, 0, err
	}

	if len(values) == 0 {
		return nil, 0, errs.ErrDimensionNodeNotFound
	}

	return values, len(values), nil
}

// UpsertDimensionsToInstance to the dimension collection
func (m *Mongo) UpsertDimensionsToInstance(opts []*models.CachedDimensionOption) error {
	s := m.Session.Copy()
	defer s.Close()

	bulk := s.DB(m.Database).C(dimensionOptions).Bulk()

	// queue all options to be upserted
	now := time.Now().UTC()
	for _, opt := range opts {
		option := models.DimensionOption{InstanceID: opt.InstanceID, Option: opt.Option, Name: opt.Name, Label: opt.Label}
		option.Order = opt.Order
		option.Links.CodeList = models.LinkObject{ID: opt.CodeList, HRef: fmt.Sprintf("%s/code-lists/%s", m.CodeListURL, opt.CodeList)}
		option.Links.Code = models.LinkObject{ID: opt.Code, HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", m.CodeListURL, opt.CodeList, opt.Code)}
		option.LastUpdated = now
		bulk.Upsert(
			bson.M{"instance_id": option.InstanceID, "name": option.Name, "option": option.Option},
			&option,
		)
	}

	// execute the upserts in bulk
	_, err := bulk.Run()
	return err
}

// GetDimensions returns a list of all dimensions from a dataset
func (m *Mongo) GetDimensions(datasetID, versionID string) ([]bson.M, error) {
	s := m.Session.Copy()
	defer s.Close()

	// To get all unique values an aggregation is needed, as using distinct() will only return the distinct values and
	// not the documents.
	// Match by instance_id
	match := bson.M{"$match": bson.M{"instance_id": versionID}}
	// Then group the values by name.
	group := bson.M{"$group": bson.M{"_id": "$name", "doc": bson.M{"$first": "$$ROOT"}}}
	results := []bson.M{}
	err := s.DB(m.Database).C(dimensionOptions).Pipe([]bson.M{match, group}).All(&results)
	if err != nil {
		return nil, err
	}

	if len(results) < 1 {
		return nil, errs.ErrDimensionsNotFound
	}

	return results, nil
}

// GetDimensionOptions returns dimension options for a dimensions within a dataset, according to the provided limit and offest.
// Offset and limit need to be positive or zero
func (m *Mongo) GetDimensionOptions(ctx context.Context, version *models.Version, dimension string, offset, limit int) ([]*models.PublicDimensionOption, int, error) {

	s := m.Session.Copy()
	defer s.Close()

	// define selector to obtain all the dimension options for an instance
	selector := bson.M{"instance_id": version.ID, "name": dimension}

	// obtain query defining the order
	q, err := m.sortedQuery(s, selector)
	if err != nil {
		return nil, 0, err
	}

	// get total count and paginated values according to provided offset and limit
	values := []*models.PublicDimensionOption{}
	totalCount, err := QueryPage(ctx, q, offset, limit, &values)
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
func (m *Mongo) GetDimensionOptionsFromIDs(version *models.Version, dimension string, IDs []string) ([]*models.PublicDimensionOption, int, error) {
	if len(IDs) > maxIDs {
		return nil, 0, errors.New("too many IDs provided")
	}

	s := m.Session.Copy()
	defer s.Close()

	selectorAll := bson.M{"instance_id": version.ID, "name": dimension}
	selectorInList := bson.M{"instance_id": version.ID, "name": dimension, "option": bson.M{"$in": IDs}}

	// count total number of options in dimension
	totalCount, err := s.DB(m.Database).C(dimensionOptions).Find(selectorAll).Count()
	if err != nil {
		return nil, 0, err
	}

	var values []*models.PublicDimensionOption

	if totalCount > 0 {

		// obtain query defining the order for the provided IDs only
		q, err := m.sortedQuery(s, selectorInList)
		if err != nil {
			return nil, 0, err
		}

		// obtain all required options in order
		iter := q.Iter()
		if err := iter.All(&values); err != nil {
			return nil, 0, err
		}

		// update links for returned values
		for i := 0; i < len(values); i++ {
			values[i].Links.Version = *version.Links.Self
		}
	}

	return values, totalCount, nil
}

// UpdateDimensionNodeIDAndOrder to cache the id and order (optional) for other import processes
func (m *Mongo) UpdateDimensionNodeIDAndOrder(dimension *models.DimensionOption) error {

	// validate that there is something to update
	if dimension.Order == nil && dimension.NodeID == "" {
		return nil
	}

	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name, "option": dimension.Option}

	update := bson.M{"last_updated": time.Now().UTC()}
	if dimension.NodeID != "" {
		update["node_id"] = &dimension.NodeID
	}
	if dimension.Order != nil {
		update["order"] = &dimension.Order
	}

	err := s.DB(m.Database).C(dimensionOptions).Update(selector, bson.M{"$set": update})
	if err == mgo.ErrNotFound {
		return errs.ErrDimensionOptionNotFound
	}

	if err != nil {
		return err
	}

	return nil
}

// sortedQuery generates a sorted mongoDB query from the provided bson.M selector
// if order property exists, it will be used to determine the order
// otherwise, the items will be sorted alphabetically by option
func (m *Mongo) sortedQuery(s *mgo.Session, selector bson.M) (*mgo.Query, error) {
	q := s.DB(m.Database).C(dimensionOptions).Find(selector)

	selector["order"] = bson.M{"$exists": true}
	orderCount, err := s.DB(m.Database).C(dimensionOptions).Find(selector).Count()
	if err != nil {
		return nil, err
	}
	delete(selector, "order")

	if orderCount > 0 {
		return q.Sort("order"), nil
	}
	return q.Sort("option"), nil
}
