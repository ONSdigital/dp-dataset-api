package mongo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ONSdigital/dp-dataset-api/config"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"

	"github.com/ONSdigital/log.go/v2/log"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	bsonprim "go.mongodb.org/mongo-driver/bson/primitive"
)

// AcquireInstanceLock tries to lock the provided instanceID.
// If the instance is already locked, this function will block until it's released,
// at which point we acquire the lock and return.
func (m *Mongo) AcquireInstanceLock(ctx context.Context, instanceID string) (lockID string, err error) {
	return m.lockClient.Acquire(ctx, instanceID)
}

// UnlockInstance releases an exclusive mongoDB lock for the provided lockId (if it exists)
func (m *Mongo) UnlockInstance(ctx context.Context, lockID string) {
	m.lockClient.Unlock(ctx, lockID)
}

// GetInstances from a mongo collection
func (m *Mongo) GetInstances(ctx context.Context, states, datasets []string, offset, limit int) ([]*models.Instance, int, error) {
	selector := bson.M{}
	if len(states) > 0 {
		selector["state"] = bson.M{"$in": states}
	}
	if len(datasets) > 0 {
		selector["links.dataset.id"] = bson.M{"$in": datasets}
	}

	// get total count and paginated values according to provided offset and limit
	results := []*models.Instance{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Find(ctx, selector, &results,
		mongodriver.Sort(bson.M{"last_updated": -1}),
		mongodriver.Offset(offset),
		mongodriver.Limit(limit))
	if err != nil {
		return results, 0, err
	}

	return results, totalCount, nil
}

// GetInstance returns a single instance from an ID
func (m *Mongo) GetInstance(ctx context.Context, id, eTagSelector string) (*models.Instance, error) {
	// get instance from DB
	var instance models.Instance
	if err := m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).FindOne(ctx, bson.M{"id": id}, &instance); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
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
func (m *Mongo) AddInstance(ctx context.Context, instance *models.Instance) (inst *models.Instance, err error) {
	instance.LastUpdated = time.Now().UTC()

	unixTime := instance.LastUpdated.Unix()
	if unixTime < 0 || unixTime > math.MaxUint32 {
		return nil, fmt.Errorf("timestamp out of range for uint32: %d", unixTime)
	}
	instance.UniqueTimestamp = bsonprim.Timestamp{T: uint32(unixTime)}

	// set eTag value to current hash of the instance
	instance.ETag, err = instance.Hash(nil)
	if err != nil {
		return nil, err
	}

	// Insert instance to database
	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Insert(ctx, &instance); err != nil {
		return nil, err
	}

	return instance, nil
}

// UpdateInstance with new properties
func (m *Mongo) UpdateInstance(ctx context.Context, currentInstance, updatedInstance *models.Instance, eTagSelector string) (newETag string, err error) {
	// set lastUpdate value to now
	updatedInstance.LastUpdated = time.Now().UTC()

	// calculate the new eTag hash for the instance that would result from applying the update
	newETag, err = newETagForUpdate(currentInstance, updatedInstance)
	if err != nil {
		return "", err
	}
	updatedInstance.ETag = newETag

	// create update query from updatedInstance and newly generated eTag
	updates := createInstanceUpdateQuery(ctx, currentInstance.InstanceID, updatedInstance)
	update := bson.M{"$set": updates}
	updateWithTimestamps, err := mongodriver.WithUpdates(update)
	if err != nil {
		return "", err
	}

	// create selector query
	sel := selector(currentInstance.InstanceID, updatedInstance.UniqueTimestamp, eTagSelector)

	// execute the update against MongoDB to atomically check and update the instance
	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel, updateWithTimestamps); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrConflictUpdatingInstance
		}
		return "", err
	}

	return newETag, nil
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func createInstanceUpdateQuery(ctx context.Context, instanceID string, instance *models.Instance) bson.M {
	updates := make(bson.M)

	logData := log.Data{"instance_id": instanceID}

	log.Info(ctx, "building update query for instance resource", logData)

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

	if instance.IsBasedOn != nil {
		if instance.IsBasedOn.ID != "" {
			updates["is_based_on.id"] = instance.IsBasedOn.ID
		}
		if instance.IsBasedOn.Type != "" {
			updates["is_based_on.type"] = instance.IsBasedOn.Type
		}
	}

	logData["updates"] = updates
	log.Info(ctx, "built update query for instance resource", logData)

	return updates
}

// AddEventToInstance to the instance collection
func (m *Mongo) AddEventToInstance(ctx context.Context, currentInstance *models.Instance, event *models.Event, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from adding the event
	newETag, err = newETagForAddEvent(currentInstance, event)
	if err != nil {
		return "", err
	}

	// create selector query
	sel := selector(currentInstance.InstanceID, bsonprim.Timestamp{}, eTagSelector)

	update := bson.M{
		"$push": bson.M{"events": &event},
		"$set": bson.M{
			"last_updated": time.Now().UTC(),
			"e_tag":        newETag,
		},
	}

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel, update); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrInstanceNotFound
		}
		return "", err
	}

	return newETag, nil
}

// UpdateObservationInserted by incrementing the stored value
func (m *Mongo) UpdateObservationInserted(ctx context.Context, currentInstance *models.Instance, observationInserted int64, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForObservationsInserted(currentInstance, observationInserted)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, bsonprim.Timestamp{}, eTagSelector)
	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel,
		bson.M{
			"$inc": bson.M{"import_tasks.import_observations.total_inserted_observations": observationInserted},
			"$set": bson.M{
				"last_updated": time.Now().UTC(),
				"e_tag":        newETag,
			},
		},
	); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrInstanceNotFound
		}
		return "", err
	}

	return newETag, nil
}

// UpdateImportObservationsTaskState to the given state.
func (m *Mongo) UpdateImportObservationsTaskState(ctx context.Context, currentInstance *models.Instance, state, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForStateUpdate(currentInstance, state)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, bsonprim.Timestamp{}, eTagSelector)

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel,
		bson.M{
			"$set": bson.M{
				"import_tasks.import_observations.state": state,
				"e_tag":                                  newETag,
			},
			"$currentDate": bson.M{"last_updated": true},
		},
	); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrInstanceNotFound
		}
		return "", err
	}

	return newETag, nil
}

// UpdateBuildHierarchyTaskState updates the state of a build hierarchy task.
func (m *Mongo) UpdateBuildHierarchyTaskState(ctx context.Context, currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForHierarchyTaskStateUpdate(currentInstance, dimension, state)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, bsonprim.Timestamp{}, eTagSelector)
	sel["import_tasks.build_hierarchies.dimension_name"] = dimension

	update := bson.M{
		"$set": bson.M{
			"import_tasks.build_hierarchies.$.state": state,
			"e_tag":                                  newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel, update); err != nil {
		return "", err
	}

	return newETag, nil
}

// UpdateBuildSearchTaskState updates the state of a build search task.
func (m *Mongo) UpdateBuildSearchTaskState(ctx context.Context, currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from inceasing the observations
	newETag, err = newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, state)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, bsonprim.Timestamp{}, eTagSelector)
	sel["import_tasks.build_search_indexes.dimension_name"] = dimension

	update := bson.M{
		"$set": bson.M{
			"import_tasks.build_search_indexes.$.state": state,
			"e_tag": newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel, update); err != nil {
		return "", err
	}

	return newETag, nil
}

// UpdateETagForOptions updates the eTag value for an instance according to the provided dimension options upserts and updates
func (m *Mongo) UpdateETagForOptions(ctx context.Context, currentInstance *models.Instance, upserts []*models.CachedDimensionOption, updates []*models.DimensionOption, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash by calculating the hash of the current instance plus the provided option upserts and updates
	newETag, err = newETagForOptions(currentInstance, upserts, updates)
	if err != nil {
		return "", err
	}

	sel := selector(currentInstance.InstanceID, bsonprim.Timestamp{}, eTagSelector)

	update := bson.M{
		"$set": bson.M{
			"e_tag": newETag,
		},
		"$currentDate": bson.M{"last_updated": true},
	}

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel, update); err != nil {
		return "", err
	}

	return newETag, nil
}

// selector creates a select query for mongoDB with the provided parameters
// - instanceID represents the ID of the instance document that we want to query. Required.
// - timestamp is a unique MongoDB timestamp to be matched to prevent race conditions. Optional.
// - eTagselector is a unique hash of an instance document to be matched to prevent race conditions. Optional.
func selector(instanceID string, timestamp bsonprim.Timestamp, eTagSelector string) bson.M {
	selector := bson.M{"id": instanceID}
	if !timestamp.IsZero() {
		selector[mongodriver.UniqueTimestampKey] = timestamp
	}
	if eTagSelector != AnyETag {
		selector["e_tag"] = eTagSelector
	}
	return selector
}
