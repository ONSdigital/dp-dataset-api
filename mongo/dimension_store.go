package mongo

import (
	"fmt"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"gopkg.in/mgo.v2/bson"
	"time"
)

const DIMENSION_OPTIONS = "dimension.options"

// GetDimensionNodesFromInstance which are stored in a mongodb collection
func (m *Mongo) GetDimensionNodesFromInstance(id string) (*models.DimensionNodeResults, error) {
	s := session.Copy()
	defer s.Close()
	var dimensions []models.DimensionOption
	iter := s.DB(m.Database).C(DIMENSION_OPTIONS).Find(bson.M{"instance_id": id}).Select(bson.M{"id": 0, "last_updated": 0, "instance_id": 0}).Iter()
	err := iter.All(&dimensions)
	if err != nil {
		return nil, err
	}

	return &models.DimensionNodeResults{Items: dimensions}, nil
}

// GetUniqueDimensionValues which are stored in mongodb collection
func (m *Mongo) GetUniqueDimensionValues(id, dimension string) (*models.DimensionValues, error) {
	s := session.Copy()
	defer s.Close()
	var values []string
	err := s.DB(m.Database).C(DIMENSION_OPTIONS).Find(bson.M{"instance_id": id, "name": dimension}).Distinct("value", &values)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, errs.DimensionNodeNotFound
	}
	return &models.DimensionValues{Name: dimension, Values: values}, nil
}

// AddDimensionToInstance to the dimension collection
func (m *Mongo) AddDimensionToInstance(opt *models.CachedDimensionOption) error {
	s := session.Copy()
	defer s.Close()
	option := models.DimensionOption{InstanceID: opt.InstanceID, Value: opt.Value, Label: opt.Label}
	option.Links.CodeList = models.LinkObject{ID: opt.CodeList, HRef: fmt.Sprintf("%s/%s", m.CodeListURL, opt.CodeList)}
	option.Links.Code = models.LinkObject{ID: opt.Value, HRef: fmt.Sprintf("%s/%s/codes/%s", m.CodeListURL, opt.CodeList, opt.Value)}
	option.LastUpdated = time.Now().UTC()
	_, err := s.DB(m.Database).C(DIMENSION_OPTIONS).Upsert(bson.M{"instance_id": option.InstanceID, "name": option.Name,
		"value": option.Value}, &option)
	if err != nil {
		return err
	}
	return nil
}

// GetDimensions returns a list of all dimensions from a dataset
func (m *Mongo) GetDimensions(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error) {
	s := session.Copy()
	defer s.Close()
	//version, err := m.GetVersion(datasetID, editionID, versionID, "published")
	//if err != nil {
	//	return nil, err
	//}
	var results []models.Dimension
	match := bson.M{"$match": bson.M{"instance_id": "665BEE8A-D88F-448C-BF29-186D18B8DABE"}}
	group := bson.M{"$group": bson.M{"_id": "$name", "doc": bson.M{"$first": "$$ROOT"}}}
	res := []bson.M{}
	err := s.DB(m.Database).C(DIMENSION_OPTIONS).Pipe([]bson.M{match, group}).All(&res)
	if err != nil {
		return nil, err
	}
	for _, dim := range res {
		opt :=  convertBSonToDimension(dim["doc"])
		dimension := models.Dimension{Name: opt.Name}
		dimension.Links.CodeList = opt.Links.CodeList
		//dimension.Links.Edition = version.Links.Edition
		//dimension.Links.Dataset = version.Links.Dataset
		//dimension.Links.Version = version.Links.Self
		results = append(results, dimension)
	}
	return &models.DatasetDimensionResults{Items: results}, nil
}

// GetDimensionOptions returns all dimension options for a dimensions within a dataset.
func (m *Mongo) GetDimensionOptions(datasetID, editionID, versionID, dimension string) (*models.DimensionOptionResults, error) {
	s := session.Copy()
	defer s.Close()
	//version, err := m.GetVersion(datasetID, editionID, versionID, "published")
	//if err != nil {
	//	return nil, err
	//}
	instanceId := "665BEE8A-D88F-448C-BF29-186D18B8DABE" //version.InstanceID
	var values []models.PublicDimensionOption
	iter := s.DB(m.Database).C(DIMENSION_OPTIONS).Find(bson.M{"instance_id": instanceId, "name": dimension}).Iter()
	err := iter.All(&values)
	if err != nil {
		return nil, err
	}

	return &models.DimensionOptionResults{Items: values}, nil
}

func convertBSonToDimension(data interface{}) *models.DimensionOption {
	var dim models.DimensionOption
    bytes , err:= bson.Marshal(data)
	if err != nil {

	}
	bson.Unmarshal(bytes, &dim)

	return &dim
}
