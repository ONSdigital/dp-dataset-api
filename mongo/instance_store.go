package mongo

import (
	"context"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dpmongo "github.com/ONSdigital/dp-mongodb"
	"github.com/ONSdigital/log.go/log"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// AcquireInstanceLock tries to lock the provided instanceID.
// If the instance is already locked, this function will block until it's released,
// at which point we acquire the lock and return.
func (m *Mongo) AcquireInstanceLock(ctx context.Context, instanceID string) (lockID string, err error) {
	return m.lockClient.Acquire(ctx, instanceID)
}

// UnlockInstance releases an exclusive mongoDB lock for the provided lockId (if it exists)
func (m *Mongo) UnlockInstance(lockID string) error {
	return m.lockClient.Unlock(lockID)
}

// GetInstances from a mongo collection
func (m *Mongo) GetInstances(ctx context.Context, states []string, datasets []string, offset, limit int) ([]*models.Instance, int, error) {
	s := m.Session.Copy()
	defer s.Close()

	selector := bson.M{}
	if len(states) > 0 {
		selector["state"] = bson.M{"$in": states}
	}

	if len(datasets) > 0 {
		selector["links.dataset.id"] = bson.M{"$in": datasets}
	}

	q := s.DB(m.Database).C(instanceCollection).Find(selector).Sort("-last_updated")

	// get total count and paginated values according to provided offset and limit
	results := []*models.Instance{}
	totalCount, err := QueryPage(ctx, q, offset, limit, &results)
	if err != nil {
		if err == mgo.ErrNotFound {
			return results, 0, errs.ErrDatasetNotFound
		}
		return results, 0, err
	}

	return results, totalCount, nil
}

// GetInstance returns a single instance from an ID
func (m *Mongo) GetInstance(ID, eTagSelector string) (*models.Instance, error) {
	s := m.Session.Copy()
	defer s.Close()

	// get instance from DB
	var instance models.Instance
	if err := s.DB(m.Database).C(instanceCollection).Find(bson.M{"id": ID}).One(&instance); err != nil {
		if err == mgo.ErrNotFound {
			return nil, errs.ErrInstanceNotFound
		}
		return nil, err
	}

	// If eTag was provided and did not match, return the corresponding error
	if eTagSelector != AnyETag && eTagSelector != instance.ETag {
		return nil, errs.ErrInstanceConflict
	}

	return &instance, nil
}

// AddInstance to the instance collection
func (m *Mongo) AddInstance(instance *models.Instance) (*models.Instance, error) {
	s := m.Session.Copy()
	defer s.Close()

	// Initialise with timestamp
	instance.LastUpdated = time.Now().UTC()
	var err error
	if instance.UniqueTimestamp, err = bson.NewMongoTimestamp(instance.LastUpdated, 1); err != nil {
		return nil, err
	}

	// set eTag value to current hash of the instance
	instance.ETag, err = instance.Hash(nil)
	if err != nil {
		return nil, err
	}

	// Insert instance to database
	if err = s.DB(m.Database).C(instanceCollection).Insert(&instance); err != nil {
		return nil, err
	}

	return instance, nil
}

// UpdateInstance with new properties
func (m *Mongo) UpdateInstance(ctx context.Context, currentInstance, updatedInstance *models.Instance, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// set lastUpdate value to now
	updatedInstance.LastUpdated = time.Now().UTC()

	// calculate the new eTag hash for the instance that would result from applying the update
	newETag, err = newETagForUpdate(currentInstance, updatedInstance)
	if err != nil {
		return "", err
	}
	updatedInstance.ETag = newETag

	// create selector query
	sel := selector(currentInstance.InstanceID, updatedInstance.UniqueTimestamp, eTagSelector)

	// create update query from updatedInstance and newly generated eTag
	updates := createInstanceUpdateQuery(ctx, currentInstance.InstanceID, updatedInstance)
	update := bson.M{"$set": updates}
	updateWithTimestamps, err := dpmongo.WithUpdates(update)
	if err != nil {
		return "", err
	}

	// execute the update against MongoDB to atomically check and update the instance
	if err = s.DB(m.Database).C(instanceCollection).Update(sel, updateWithTimestamps); err != nil {
		if err != mgo.ErrNotFound {
			return "", err
		}

		return "", errs.ErrConflictUpdatingInstance
	}

	return newETag, nil
}

func createInstanceUpdateQuery(ctx context.Context, instanceID string, instance *models.Instance) bson.M {
	updates := make(bson.M)

	logData := log.Data{"instance_id": instanceID, "instance": instance}

	log.Event(ctx, "building update query for instance resource", log.INFO, logData)

	if instance.Alerts != nil {
		updates["alerts"] = instance.Alerts
	}

	if instance.InstanceID != "" {
		updates["id"] = instance.InstanceID
	}

	if instance.CollectionID != "" {
		updates["collection_id"] = instance.CollectionID
	}

	if instance.Dimensions != nil {
		updates["dimensions"] = instance.Dimensions
	}

	if instance.ETag != "" {
		updates["e_tag"] = instance.ETag
	}

	if instance.Downloads != nil {
		if instance.Downloads.CSV != nil {
			if instance.Downloads.CSV.HRef != "" {
				updates["downloads.csv.href"] = instance.Downloads.CSV.HRef
			}
			if instance.Downloads.CSV.Private != "" {
				updates["downloads.csv.private"] = instance.Downloads.CSV.Private
			}
			if instance.Downloads.CSV.Public != "" {
				updates["downloads.csv.public"] = instance.Downloads.CSV.Public
			}
			if instance.Downloads.CSV.Size != "" {
				updates["downloads.csv.size"] = instance.Downloads.CSV.Size
			}
		}

		if instance.Downloads.XLS != nil {
			if instance.Downloads.XLS.HRef != "" {
				updates["downloads.xls.href"] = instance.Downloads.XLS.HRef
			}
			if instance.Downloads.XLS.Private != "" {
				updates["downloads.xls.private"] = instance.Downloads.XLS.Private
			}
			if instance.Downloads.XLS.Public != "" {
				updates["downloads.xls.public"] = instance.Downloads.XLS.Public
			}
			if instance.Downloads.XLS.Size != "" {
				updates["downloads.xls.size"] = instance.Downloads.XLS.Size
			}
		}
	}

	if instance.Edition != "" {
		updates["edition"] = instance.Edition
	}

	if instance.Headers != nil && instance.Headers != &[]string{""} {
		updates["headers"] = instance.Headers
	}

	if instance.ImportTasks != nil {
		if instance.ImportTasks.BuildHierarchyTasks != nil {
			updates["import_tasks.build_hierarchies"] = instance.ImportTasks.BuildHierarchyTasks
		}
		if instance.ImportTasks.BuildSearchIndexTasks != nil {
			updates["import_tasks.build_search_indexes"] = instance.ImportTasks.BuildSearchIndexTasks
		}
		if instance.ImportTasks.ImportObservations != nil {
			updates["import_tasks.import_observations"] = instance.ImportTasks.ImportObservations
		}
	}

	if instance.LatestChanges != nil {
		updates["latest_changes"] = instance.LatestChanges
	}

	if instance.Links != nil {
		if instance.Links.Dataset != nil {
			updates["links.dataset"] = instance.Links.Dataset
		}
		if instance.Links.Dimensions != nil {
			updates["links.dimensions"] = instance.Links.Dimensions
		}
		if instance.Links.Edition != nil {
			updates["links.edition"] = instance.Links.Edition
		}
		if instance.Links.Job != nil {
			updates["links.job"] = instance.Links.Job
		}
		if instance.Links.Self != nil {
			updates["links.self"] = instance.Links.Self
		}
		if instance.Links.Spatial != nil {
			updates["links.spatial"] = instance.Links.Spatial
		}
		if instance.Links.Version != nil {
			updates["links.version"] = instance.Links.Version
		}
	}

	if instance.ReleaseDate != "" {
		updates["release_date"] = instance.ReleaseDate
	}

	if instance.State != "" {
		updates["state"] = instance.State
	}

	if instance.Temporal != nil {
		updates["temporal"] = instance.Temporal
	}

	if instance.TotalObservations != nil {
		updates["total_observations"] = instance.TotalObservations
	}

	if instance.Version != 0 {
		updates["version"] = instance.Version
	}

	logData["updates"] = updates
	log.Event(ctx, "built update query for instance resource", log.INFO, logData)

	return updates
}

// AddEventToInstance to the instance collection
func (m *Mongo) AddEventToInstance(currentInstance *models.Instance, event *models.Event, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash for the instance that would result from adding the event
	newETag, err = newETagForAddEvent(currentInstance, event)
	if err != nil {
		return "", err
	}

	// create selector query
	sel := selector(currentInstance.InstanceID, 0, eTagSelector)

	update := bson.M{
		"$push": bson.M{"events": &event},
		"$set": bson.M{
			"last_updated": time.Now().UTC(),
			"e_tag":        newETag,
		},
	}

	info, err := s.DB(m.Database).C(instanceCollection).Upsert(sel, update)
	if err != nil {
		return "", err
	}

	if info.Updated == 0 {
		return "", errs.ErrInstanceNotFound
	}

	return newETag, nil
}

// UpdateObservationInserted by incrementing the stored value
func (m *Mongo) UpdateObservationInserted(currentInstance *models.Instance, observationInserted int64, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForObservationsInserted(currentInstance, observationInserted)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, 0, eTagSelector)
	err = s.DB(m.Database).C(instanceCollection).Update(sel,
		bson.M{
			"$inc": bson.M{"import_tasks.import_observations.total_inserted_observations": observationInserted},
			"$set": bson.M{
				"last_updated": time.Now().UTC(),
				"e_tag":        newETag,
			},
		},
	)

	if err == mgo.ErrNotFound {
		return "", errs.ErrInstanceNotFound
	}

	if err != nil {
		return "", err
	}

	return newETag, nil
}

// UpdateImportObservationsTaskState to the given state.
func (m *Mongo) UpdateImportObservationsTaskState(currentInstance *models.Instance, state, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForStateUpdate(currentInstance, state)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, 0, eTagSelector)

	err = s.DB(m.Database).C(instanceCollection).Update(sel,
		bson.M{
			"$set": bson.M{
				"import_tasks.import_observations.state": state,
				"e_tag":                                  newETag,
			},
			"$currentDate": bson.M{"last_updated": true},
		},
	)

	if err == mgo.ErrNotFound {
		return "", errs.ErrInstanceNotFound
	}

	if err != nil {
		return "", err
	}

	return newETag, nil
}

// UpdateBuildHierarchyTaskState updates the state of a build hierarchy task.
func (m *Mongo) UpdateBuildHierarchyTaskState(currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForHierarchyTaskStateUpdate(currentInstance, dimension, state)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, 0, eTagSelector)
	sel["import_tasks.build_hierarchies.dimension_name"] = dimension

	update := bson.M{
		"$set": bson.M{
			"import_tasks.build_hierarchies.$.state": state,
			"e_tag":                                  newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if err := s.DB(m.Database).C(instanceCollection).Update(selector, update); err != nil {
		return "", err
	}

	return newETag, nil
}

// UpdateBuildSearchTaskState updates the state of a build search task.
func (m *Mongo) UpdateBuildSearchTaskState(currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, state)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, 0, eTagSelector)
	sel["import_tasks.build_search_indexes.dimension_name"] = dimension

	update := bson.M{
		"$set": bson.M{
			"import_tasks.build_search_indexes.$.state": state,
			"e_tag": newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if err := s.DB(m.Database).C(instanceCollection).Update(selector, update); err != nil {
		return "", err
	}

	return newETag, nil
}

func (m *Mongo) UpdateETagForNodeIDAndOrder(currentInstance *models.Instance, nodeID string, order *int, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash by calculating the hash of the current instance plus the provided nodeID and order
	newETag, err = newETagForNodeIDAndOrder(currentInstance, nodeID, order)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, 0, eTagSelector)

	update := bson.M{
		"$set": bson.M{
			"e_tag": newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if err := s.DB(m.Database).C(instanceCollection).Update(sel, update); err != nil {
		return "", err
	}

	return newETag, nil
}

// UpdateETagForOptions updates the eTag value for an instance according to the provided dimension options
func (m *Mongo) UpdateETagForOptions(currentInstance *models.Instance, option *models.CachedDimensionOption, eTagSelector string) (newETag string, err error) {
	s := m.Session.Copy()
	defer s.Close()

	// calculate the new eTag hash by calculating the hash of the current instance plus the provided option
	newETag, err = newETagForAddDimensionOption(currentInstance, option)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, 0, eTagSelector)

	update := bson.M{
		"$set": bson.M{
			"e_tag": newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if err := s.DB(m.Database).C(instanceCollection).Update(sel, update); err != nil {
		return "", err
	}

	return newETag, nil
}

// selector creates a select query for mongoDB with the provided parameters
// - instanceID represents the ID of the instance document that we want to query. Required.
// - timestamp is a unique MongoDB timestamp to be matched to prevent race conditions. Optional.
// - eTagselector is a unique hash of an instance document to be matched to prevent race conditions. Optional.
func selector(instanceID string, timestamp bson.MongoTimestamp, eTagSelector string) bson.M {
	selector := bson.M{"id": instanceID}
	if timestamp > 0 {
		selector[dpmongo.UniqueTimestampKey] = timestamp
	}
	if eTagSelector != AnyETag {
		selector["e_tag"] = eTagSelector
	}
	return selector
}
