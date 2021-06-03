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

const instanceCollection = "instances"

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
	totalCount, err := dpmongo.QueryPage(ctx, q, offset, limit, &results)
	if err != nil {
		if err == mgo.ErrNotFound {
			return results, 0, errs.ErrDatasetNotFound
		}
		return results, 0, err
	}

	return results, totalCount, nil
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
func (m *Mongo) UpdateInstance(ctx context.Context, instanceID string, instance *models.Instance) error {
	s := m.Session.Copy()
	defer s.Close()

	instance.LastUpdated = time.Now().UTC()

	updates := createInstanceUpdateQuery(ctx, instanceID, instance)
	update := bson.M{"$set": updates}
	updateWithTimestamps, err := dpmongo.WithUpdates(update)
	if err != nil {
		return err
	}

	if err = s.DB(m.Database).C(instanceCollection).Update(bson.M{"id": instanceID, dpmongo.UniqueTimestampKey: instance.UniqueTimestamp}, updateWithTimestamps); err != nil {
		if err != mgo.ErrNotFound {
			return err
		}

		return errs.ErrConflictUpdatingInstance
	}

	return nil
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
