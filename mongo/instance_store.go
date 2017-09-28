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

// GetInstances from a mongo collection
func (m *Mongo) GetInstances(filter string) (*models.InstanceResults, error) {
	s := m.Session.Copy()
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
	s := m.Session.Copy()
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
	s := m.Session.Copy()
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
	s := m.Session.Copy()
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
	s := m.Session.Copy()
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

// UpdateDimensionNodeID to cache the id for other import processes
func (m *Mongo) UpdateDimensionNodeID(dimension *models.DimensionOption) error {
	s := session.Copy()
	defer s.Close()
	err := s.DB(m.Database).C(DIMENSION_OPTIONS).Update(bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name,
		"option":                                                              dimension.Option}, bson.M{"$set": bson.M{"node_id": &dimension.NodeID, "last_updated": time.Now().UTC()}})
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
	s := m.Session.Copy()
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

