package mongo

import (
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/satori/go.uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const INSTANCE_COLLECTION = "instances"
const CACHED_DIMENSION_COLLECTION = "cached.dimension"

// GetInstances from a mongo collection
func (m *Mongo) GetInstances(filter string) (*models.InstanceResults, error) {
	s := session.Copy()
	defer s.Close()
	query := bson.M{}
	if filter != "" {
		query["state"] = filter
	}
	iter := s.DB(m.Database).C(INSTANCE_COLLECTION).Find(query).Iter()
	defer iter.Close()

	results := []models.Instance{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.DatasetNotFound
		}
		return nil, err
	}

	return &models.InstanceResults{Items: results}, nil
}

// GetInstance returns a single instance from an ID
func (m *Mongo) GetInstance(ID string) (*models.Instance, error) {
	s := session.Copy()
	defer s.Close()
	var instance models.Instance
	err := s.DB(m.Database).C(INSTANCE_COLLECTION).Find(bson.M{"id": ID}).One(&instance)

	if err == mgo.ErrNotFound {
		return nil, errs.InstanceNotFound
	}

	return &instance, err
}

// AddInstance to the instance collection
func (m *Mongo) AddInstance(instance *models.Instance) (*models.Instance, error) {
	s := session.Copy()
	defer s.Close()

	instance.InstanceID = uuid.NewV4().String()
	instance.LastUpdated = time.Now().UTC()
	err := s.DB(m.Database).C(INSTANCE_COLLECTION).Insert(&instance)
	if err != nil {
		return nil, err
	}

	return instance, nil
}

// UpdateInstance with new properties
func (m *Mongo) UpdateInstance(id string, instance *models.Instance) error {
	s := session.Copy()
	defer s.Close()

	instance.InstanceID = id
	instance.LastUpdated = time.Now().UTC()

	info, err := s.DB(m.Database).C(INSTANCE_COLLECTION).Upsert(bson.M{"id": id}, bson.M{"$set": &instance})
	if err != nil {
		return err
	}
	if info.Updated == 0 {
		return errs.InstanceNotFound
	}

	return nil
}

// AddEventToInstance to the instance collection
func (m *Mongo) AddEventToInstance(instanceId string, event *models.Event) error {
	s := session.Copy()
	defer s.Close()

	info, err := s.DB(m.Database).C(INSTANCE_COLLECTION).Upsert(bson.M{"id": instanceId},
		bson.M{"$push": bson.M{"events": &event}, "$set": bson.M{"last_updated": time.Now().UTC()}})
	if err != nil {
		return err
	}
	if info.Updated == 0 {
		return errs.InstanceNotFound
	}

	return nil
}

// AddDimensionToInstance to the dimension collection
func (m *Mongo) AddDimensionToInstance(dimension *models.CachedDimension) error {
	s := session.Copy()
	defer s.Close()
	now := time.Now().UTC()
	dimension.LastUpdated = &now
	_, err := s.DB(m.Database).C(CACHED_DIMENSION_COLLECTION).Upsert(bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name,
		"value": dimension.Value}, &dimension)
	if err != nil {
		return err
	}
	return nil
}

// UpdateDimensionNodeID to cache the id for other import processes
func (m *Mongo) UpdateDimensionNodeID(dimension *models.CachedDimension) error {
	s := session.Copy()
	defer s.Close()
	err := s.DB(m.Database).C(CACHED_DIMENSION_COLLECTION).Update(bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name,
		"value": dimension.Value}, bson.M{"$set": bson.M{"node_id": &dimension.NodeID, "last_updated": time.Now().UTC()}})
	if err == mgo.ErrNotFound {
		return errs.InstanceNotFound
	}
	if err != nil {
		return err
	}
	return nil
}

// UpdateObservationInserted by incrementing the stored value
func (m *Mongo) UpdateObservationInserted(id string, observationInserted int64) error {
	s := session.Copy()
	defer s.Close()
	err := s.DB(m.Database).C(INSTANCE_COLLECTION).Update(bson.M{"id": id},
		bson.M{"$inc": bson.M{"total_inserted_observations": observationInserted}, "$set": bson.M{"last_updated": time.Now().UTC()}})

	if err == mgo.ErrNotFound {
		return errs.InstanceNotFound
	}

	if err != nil {
		return err
	}
	return nil
}

// GetDimensionNodesFromInstance which are stored in a mongodb collection
func (m *Mongo) GetDimensionNodesFromInstance(id string) (*models.DimensionNodeResults, error) {
	s := session.Copy()
	defer s.Close()
	var dimensions []models.CachedDimension
	iter := s.DB(m.Database).C(CACHED_DIMENSION_COLLECTION).Find(bson.M{"instance_id": id}).Select(bson.M{"id": 0, "last_updated": 0, "instance_id": 0}).Iter()
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
	err := s.DB(m.Database).C(CACHED_DIMENSION_COLLECTION).Find(bson.M{"instance_id": id, "name": dimension}).Distinct("value", &values)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, errs.DimensionNodeNotFound
	}
	return &models.DimensionValues{Name: dimension, Values: values}, nil
}
