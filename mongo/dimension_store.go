package mongo

import (
	"errors"
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const dimensionOptions = "dimension.options"
const maxIDs = 1000

// GetDimensionsFromInstance returns a list of dimensions and their options for an instance resource
func (m *Mongo) GetDimensionsFromInstance(id string) (*models.DimensionNodeResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	var dimensions []models.DimensionOption
	iter := s.DB(m.Database).C(dimensionOptions).Find(bson.M{"instance_id": id}).Select(bson.M{"id": 0, "last_updated": 0, "instance_id": 0}).Iter()

	err := iter.All(&dimensions)
	if err != nil {
		return nil, err
	}

	return &models.DimensionNodeResults{Items: dimensions}, nil
}

// GetUniqueDimensionAndOptions returns a list of dimension options for an instance resource
func (m *Mongo) GetUniqueDimensionAndOptions(id, dimension string) (*models.DimensionValues, error) {
	s := m.Session.Copy()
	defer s.Close()

	q, err := m.sortedQuery(s, bson.M{"instance_id": id, "name": dimension})
	if err != nil {
		return nil, err
	}

	var values []string
	err = q.Distinct("option", &values)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, errs.ErrDimensionNodeNotFound
	}

	return &models.DimensionValues{Name: dimension, Options: values}, nil
}

// AddDimensionToInstance to the dimension collection
func (m *Mongo) AddDimensionToInstance(opt *models.CachedDimensionOption) error {
	s := m.Session.Copy()
	defer s.Close()

	option := models.DimensionOption{InstanceID: opt.InstanceID, Option: opt.Option, Name: opt.Name, Label: opt.Label}
	option.Links.CodeList = models.LinkObject{ID: opt.CodeList, HRef: fmt.Sprintf("%s/code-lists/%s", m.CodeListURL, opt.CodeList)}
	option.Links.Code = models.LinkObject{ID: opt.Code, HRef: fmt.Sprintf("%s/code-lists/%s/codes/%s", m.CodeListURL, opt.CodeList, opt.Code)}

	option.LastUpdated = time.Now().UTC()
	_, err := s.DB(m.Database).C(dimensionOptions).Upsert(bson.M{"instance_id": option.InstanceID, "name": option.Name,
		"option": option.Option}, &option)

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
// Offset and limit need to be positive or zero. Zero limit is equivalent to no limit (all items starting at offset will be returned)
func (m *Mongo) GetDimensionOptions(version *models.Version, dimension string, offset, limit int) (*models.DimensionOptionResults, error) {
	if offset < 0 || limit < 0 {
		return nil, errors.New("offset and limit must be positive or zero")
	}

	s := m.Session.Copy()
	defer s.Close()

	// define selector to obtain all the dimension options for an instance
	selector := bson.M{"instance_id": version.ID, "name": dimension}

	// get total count of items
	totalCount, err := s.DB(m.Database).C(dimensionOptions).Find(selector).Count()
	if err != nil {
		return nil, err
	}

	var values []models.PublicDimensionOption

	if limit > 0 && totalCount > 0 {

		// obtain query defining the order
		q, err := m.sortedQuery(s, selector)
		if err != nil {
			return nil, err
		}

		// obtain only the necessary items according to offset and limit
		iter := q.Skip(offset).Limit(limit).Iter()
		if err := iter.All(&values); err != nil {
			return nil, err
		}

		// update links for returned values
		for i := 0; i < len(values); i++ {
			values[i].Links.Version = *version.Links.Self
		}
	}

	return &models.DimensionOptionResults{
		Items:      values,
		Count:      len(values),
		TotalCount: totalCount,
		Offset:     offset,
		Limit:      limit,
	}, nil
}

// GetDimensionOptionsFromIDs returns dimension options for a dimension within a dataset, whose IDs match the provided list of IDs
func (m *Mongo) GetDimensionOptionsFromIDs(version *models.Version, dimension string, IDs []string) (*models.DimensionOptionResults, error) {
	if len(IDs) > maxIDs {
		return nil, errors.New("too many IDs provided")
	}

	s := m.Session.Copy()
	defer s.Close()

	selectorAll := bson.M{"instance_id": version.ID, "name": dimension}
	selectorInList := bson.M{"instance_id": version.ID, "name": dimension, "option": bson.M{"$in": IDs}}

	// count total number of options in dimension
	totalCount, err := s.DB(m.Database).C(dimensionOptions).Find(selectorAll).Count()
	if err != nil {
		return nil, err
	}

	var values []models.PublicDimensionOption

	if totalCount > 0 {

		// obtain query defining the order for the provided IDs only
		q, err := m.sortedQuery(s, selectorInList)
		if err != nil {
			return nil, err
		}

		// obtain all required options in order
		iter := q.Iter()
		if err := iter.All(&values); err != nil {
			return nil, err
		}

		// update links for returned values
		for i := 0; i < len(values); i++ {
			values[i].Links.Version = *version.Links.Self
		}
	}

	return &models.DimensionOptionResults{
		Items:      values,
		Count:      len(values),
		TotalCount: totalCount,
		Offset:     0,
		Limit:      0,
	}, nil
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
