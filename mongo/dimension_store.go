package mongo

import (
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/globalsign/mgo/bson"
)

const dimensionOptions = "dimension.options"

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

	var values []string
	err := s.DB(m.Database).C(dimensionOptions).Find(bson.M{"instance_id": id, "name": dimension}).Distinct("option", &values)
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
// Offset and limit will not be validated, assuming they are positive or zero. Zero limit is equivalent to no limit (all items starting at offset will be returned)
func (m *Mongo) GetDimensionOptions(version *models.Version, dimension string, offset, limit int) (*models.DimensionOptionResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	q := s.DB(m.Database).C(dimensionOptions).Find(bson.M{"instance_id": version.ID, "name": dimension})
	totalCount, err := q.Count()
	if err != nil {
		return nil, err
	}

	var values []models.PublicDimensionOption
	iter := q.Sort("option").Skip(offset).Limit(limit).Iter()
	if err := iter.All(&values); err != nil {
		return nil, err
	}

	for i := 0; i < len(values); i++ {
		values[i].Links.Version = *version.Links.Self
	}

	return &models.DimensionOptionResults{
		Items:      values,
		Count:      len(values),
		TotalCount: totalCount,
		Offset:     offset,
		Limit:      limit,
	}, nil
}
