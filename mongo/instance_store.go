package mongo

import (
	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"gopkg.in/mgo.v2"
	"github.com/satori/go.uuid"
)

const INSTANCE_COLLECTION = "instances"

// GetInstances from a mongo collection
func (m *Mongo) GetInstances() (*models.InstanceResults, error) {
	s := session.Copy()
	defer s.Close()

	iter := s.DB(m.Database).C(INSTANCE_COLLECTION).Find(nil).Iter()

	results := []models.Instance{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, api_errors.DatasetNotFound
		}
		return nil, err
	}

	return &models.InstanceResults{Items: results}, nil
}

func (m *Mongo) AddInstance(instance *models.Instance) (*models.Instance, error) {
	s := session.Copy()
	defer s.Close()

	instance.InstanceID = uuid.NewV4().String()

	err := s.DB(m.Database).C(INSTANCE_COLLECTION).Insert(&instance)
    if err != nil {
		return nil, err
	}

	return instance, nil
}
