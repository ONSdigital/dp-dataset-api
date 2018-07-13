package mongo

import (
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/mongo"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const instanceCollection = "instances"

// GetInstances from a mongo collection
func (m *Mongo) GetInstances(filters []string) (*models.InstanceResults, error) {
	s := m.Session.Copy()
	defer s.Close()

	var stateFilter bson.M
	if len(filters) > 0 {
		stateFilter = bson.M{"state": bson.M{"$in": filters}}
	}

	iter := s.DB(m.Database).C(instanceCollection).Find(stateFilter).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			log.ErrorC("error closing iterator", err, log.Data{"data": filters})
		}
	}()

	results := []models.Instance{}
	if err := iter.All(&results); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrDatasetNotFound
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
	err := s.DB(m.Database).C(instanceCollection).Find(bson.M{"id": ID}).One(&instance)

	if err == mgo.ErrNotFound {
		return nil, errs.ErrInstanceNotFound
	}

	return &instance, err
}

// AddInstance to the instance collection
func (m *Mongo) AddInstance(instance *models.Instance) (*models.Instance, error) {
	s := m.Session.Copy()
	defer s.Close()

	instance.LastUpdated = time.Now().UTC()
	var err error
	if instance.UniqueTimestamp, err = bson.NewMongoTimestamp(instance.LastUpdated, 1); err != nil {
		return nil, err
	}
	if err = s.DB(m.Database).C(instanceCollection).Insert(&instance); err != nil {
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

	update := bson.M{"$set": &instance}
	updateWithTimestamps, err := mongo.WithUpdates(update)
	if err != nil {
		return err
	}

	info, err := s.DB(m.Database).C(instanceCollection).Upsert(bson.M{"id": id, mongo.UniqueTimestampKey: instance.UniqueTimestamp}, updateWithTimestamps)
	if err != nil {
		return err
	}

	if info.Updated == 0 {
		return errs.ErrInstanceNotFound
	}

	return nil
}

// AddEventToInstance to the instance collection
func (m *Mongo) AddEventToInstance(instanceID string, event *models.Event) error {
	s := m.Session.Copy()
	defer s.Close()

	info, err := s.DB(m.Database).C(instanceCollection).Upsert(bson.M{"id": instanceID},
		bson.M{"$push": bson.M{"events": &event}, "$set": bson.M{"last_updated": time.Now().UTC()}})
	if err != nil {
		return err
	}

	if info.Updated == 0 {
		return errs.ErrInstanceNotFound
	}

	return nil
}

// UpdateDimensionNodeID to cache the id for other import processes
func (m *Mongo) UpdateDimensionNodeID(dimension *models.DimensionOption) error {
	s := m.Session.Copy()
	defer s.Close()

	err := s.DB(m.Database).C(dimensionOptions).Update(bson.M{"instance_id": dimension.InstanceID, "name": dimension.Name,
		"option": dimension.Option}, bson.M{"$set": bson.M{"node_id": &dimension.NodeID, "last_updated": time.Now().UTC()}})
	if err == mgo.ErrNotFound {
		return errs.ErrDimensionOptionNotFound
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

	err := s.DB(m.Database).C(instanceCollection).Update(bson.M{"id": id},
		bson.M{
			"$inc": bson.M{"import_tasks.import_observations.total_inserted_observations": observationInserted},
			"$set": bson.M{"last_updated": time.Now().UTC()},
		},
	)

	if err == mgo.ErrNotFound {
		return errs.ErrInstanceNotFound
	}

	if err != nil {
		return err
	}

	return nil
}

// UpdateImportObservationsTaskState to the given state.
func (m *Mongo) UpdateImportObservationsTaskState(id string, state string) error {
	s := m.Session.Copy()
	defer s.Close()

	err := s.DB(m.Database).C(instanceCollection).Update(bson.M{"id": id},
		bson.M{
			"$set":         bson.M{"import_tasks.import_observations.state": state},
			"$currentDate": bson.M{"last_updated": true},
		},
	)

	if err == mgo.ErrNotFound {
		return errs.ErrInstanceNotFound
	}

	if err != nil {
		return err
	}

	return nil
}

// UpdateBuildHierarchyTaskState updates the state of a build hierarchy task.
func (m *Mongo) UpdateBuildHierarchyTaskState(id, dimension, state string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{
		"id": id,
		"import_tasks.build_hierarchies.dimension_name": dimension,
	}

	update := bson.M{
		"$set":         bson.M{"import_tasks.build_hierarchies.$.state": state},
		"$currentDate": bson.M{"last_updated": true},
	}

	err = s.DB(m.Database).C(instanceCollection).Update(selector, update)
	return
}

// UpdateBuildSearchTaskState updates the state of a build search task.
func (m *Mongo) UpdateBuildSearchTaskState(id, dimension, state string) (err error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{
		"id": id,
		"import_tasks.build_search_indexes.dimension_name": dimension,
	}

	update := bson.M{
		"$set":         bson.M{"import_tasks.build_search_indexes.$.state": state},
		"$currentDate": bson.M{"last_updated": true},
	}

	err = s.DB(m.Database).C(instanceCollection).Update(selector, update)
	return
}
