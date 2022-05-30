package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	bsonprim "go.mongodb.org/mongo-driver/bson/primitive"
)

// GetDatasetsByBasedOn checks Published...... TODO: FINISH
// Filter condition checks for ... .TODO: FINISH
func (m *Mongo) GetDatasetsByBasedOn(ctx context.Context, id string, offset, limit int, authorised bool) (values []*models.DatasetUpdate, totalCount int, err error) {
	var filter = bson.M{
		"$or": bson.A{
			bson.M{"current.is_based_on.id": id},
			bson.M{"next.is_based_on.id": id},
		},
	}

	if !authorised {
		filter["current"] = bson.M{"current": bson.M{"$exists": true}}
	}
	values = []*models.DatasetUpdate{}
	totalCount, err = m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).Find(ctx, filter, &values,
		mongodriver.Sort(bson.M{"_id": -1}), mongodriver.Offset(offset), mongodriver.Limit(limit))
	if err != nil {
		return nil, 0, err
	}

	if len(values) == 0 {
		return nil, 0, apierrors.ErrDatasetNotFound
	}

	return values, totalCount, nil

}

// GetDatasets retrieves all dataset documents
func (m *Mongo) GetDatasets(ctx context.Context, offset, limit int, authorised bool) (values []*models.DatasetUpdate, totalCount int, err error) {

	var filter interface{}
	if authorised {
		filter = bson.M{}
	} else {
		filter = bson.M{"current": bson.M{"$exists": true}}
	}

	values = []*models.DatasetUpdate{}
	totalCount, err = m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).Find(ctx, filter, &values,
		mongodriver.Sort(bson.M{"_id": -1}), mongodriver.Offset(offset), mongodriver.Limit(limit))
	if err != nil {
		return nil, 0, err
	}

	return values, totalCount, nil
}

// GetDataset retrieves a dataset document
func (m *Mongo) GetDataset(ctx context.Context, id string) (*models.DatasetUpdate, error) {

	var dataset models.DatasetUpdate
	err := m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).FindOne(ctx, bson.M{"_id": id}, &dataset)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}

	return &dataset, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetEditions(ctx context.Context, id, state string, offset, limit int, authorised bool) ([]*models.EditionUpdate, int, error) {

	log.Info(context.TODO(), "[DEBUG] getting editions", log.Data{})

	selector := buildEditionsQuery(id, state, authorised)
	log.Info(context.TODO(), "[DEBUG] query details", log.Data{
		"editionsCollection": m.ActualCollectionName(config.EditionsCollection),
		"selector":           selector,
		"database":           m.Database,
	})

	// get total count and paginated values according to provided offset and limit
	results := []*models.EditionUpdate{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.EditionsCollection)).Find(ctx, selector, &results,
		mongodriver.Sort(bson.M{"_id": 1}), mongodriver.Offset(offset), mongodriver.Limit(limit))
	if err != nil {
		return results, 0, err
	}

	if totalCount < 1 {
		return nil, 0, errs.ErrEditionNotFound
	}

	return results, totalCount, nil
}

func buildEditionsQuery(id, state string, authorised bool) bson.M {

	log.Info(context.TODO(), "[DEBUG] building query", log.Data{"id": id, "state": state, "authorised": authorised})
	// all queries must get the dataset by id
	selector := bson.M{
		"next.links.dataset.id": id,
	}

	// non-authorised queries require that the current edition must exist
	if !authorised {
		selector["current"] = bson.M{"$exists": true}
	}

	// if state is required, then we need to query by state
	if state != "" {
		selector["current.state"] = state
	}

	return selector
}

// GetEdition retrieves an edition document for a dataset
func (m *Mongo) GetEdition(ctx context.Context, id, editionID, state string) (*models.EditionUpdate, error) {

	selector := buildEditionQuery(id, editionID, state)

	var edition models.EditionUpdate
	err := m.Connection.Collection(m.ActualCollectionName(config.EditionsCollection)).FindOne(ctx, selector, &edition)

	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrEditionNotFound
		}
		return nil, err
	}
	return &edition, nil
}

func buildEditionQuery(id, editionID, state string) bson.M {
	var selector bson.M
	if state != "" {
		selector = bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}
	} else {
		selector = bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
		}
	}

	return selector
}

// GetNextVersion retrieves the latest version for an edition of a dataset
func (m *Mongo) GetNextVersion(ctx context.Context, datasetID, edition string) (int, error) {

	var version models.Version
	var nextVersion int

	selector := bson.M{
		"links.dataset.id": datasetID,
		"edition":          edition,
	}

	// Results are sorted in reverse order to get latest version
	err := m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).FindOne(ctx, selector, &version, mongodriver.Sort(bson.M{"version": -1}))
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return 1, nil
		}
		return nextVersion, err
	}

	nextVersion = version.Version + 1

	return nextVersion, nil
}

// GetVersions retrieves all version documents for a dataset edition
func (m *Mongo) GetVersions(ctx context.Context, datasetID, editionID, state string, offset, limit int) ([]models.Version, int, error) {

	selector := buildVersionsQuery(datasetID, editionID, state)
	// get total count and paginated values according to provided offset and limit
	results := []models.Version{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Find(ctx, selector, &results,
		mongodriver.Sort(bson.M{"last_updated": -1}),
		mongodriver.Offset(offset),
		mongodriver.Limit(limit))
	if err != nil {
		return results, 0, err
	}

	if totalCount < 1 {
		return nil, 0, errs.ErrVersionNotFound
	}

	for i := 0; i < len(results); i++ {
		results[i].Links.Self.HRef = results[i].Links.Version.HRef
		results[i].DatasetID = datasetID
	}

	return results, totalCount, nil
}

func buildVersionsQuery(datasetID, editionID, state string) bson.M {

	var selector bson.M
	if state == "" {
		selector = bson.M{
			"links.dataset.id": datasetID,
			"edition":          editionID,
			"$or": []interface{}{
				bson.M{"state": models.EditionConfirmedState},
				bson.M{"state": models.AssociatedState},
				bson.M{"state": models.PublishedState},
			},
		}
	} else {
		selector = bson.M{
			"links.dataset.id": datasetID,
			"edition":          editionID,
			"state":            state,
		}
	}

	return selector
}

// GetVersion retrieves a version document for a dataset edition
func (m *Mongo) GetVersion(ctx context.Context, id, editionID string, versionID int, state string) (*models.Version, error) {

	selector := buildVersionQuery(id, editionID, state, versionID)

	var version models.Version
	err := m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).FindOne(ctx, selector, &version)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrVersionNotFound
		}
		return nil, err
	}

	return &version, nil
}

func buildVersionQuery(id, editionID, state string, versionID int) bson.M {

	var selector bson.M
	if state != models.PublishedState {
		selector = bson.M{
			"links.dataset.id": id,
			"version":          versionID,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
			"state":            state,
		}
	}

	return selector
}

// UpdateDataset updates an existing dataset document
func (m *Mongo) UpdateDataset(ctx context.Context, id string, dataset *models.Dataset, currentState string) (err error) {

	updates := createDatasetUpdateQuery(ctx, id, dataset, currentState)
	update := bson.M{"$set": updates, "$setOnInsert": bson.M{"next.last_updated": time.Now()}}
	if _, err = m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).Must().UpdateById(ctx, id, update); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrDatasetNotFound
		}
		return err
	}

	return nil
}

func createDatasetUpdateQuery(ctx context.Context, id string, dataset *models.Dataset, currentState string) bson.M {

	updates := make(bson.M)

	log.Info(ctx, "building update query for dataset resource", log.Data{"dataset_id": id, "dataset": dataset, "updates": updates})

	if dataset.CollectionID != "" {
		updates["next.collection_id"] = dataset.CollectionID
	}

	if dataset.Contacts != nil {
		updates["next.contacts"] = dataset.Contacts
	}

	if dataset.Description != "" {
		updates["next.description"] = dataset.Description
	}

	if dataset.Keywords != nil {
		updates["next.keywords"] = dataset.Keywords
	}

	if dataset.License != "" {
		updates["next.license"] = dataset.License
	}

	if dataset.Links != nil {
		if dataset.Links.AccessRights != nil {
			if dataset.Links.AccessRights.HRef != "" {
				updates["next.links.access_rights.href"] = dataset.Links.AccessRights.HRef
			}
		}

		if dataset.Links.Taxonomy != nil {
			if dataset.Links.Taxonomy.HRef != "" {
				updates["next.links.taxonomy.href"] = dataset.Links.Taxonomy.HRef
			}
		}
	}

	if dataset.Methodologies != nil {
		updates["next.methodologies"] = dataset.Methodologies
	}

	if dataset.NationalStatistic != nil {
		updates["next.national_statistic"] = dataset.NationalStatistic
	}

	if dataset.NextRelease != "" {
		updates["next.next_release"] = dataset.NextRelease
	}

	if dataset.Publications != nil {
		updates["next.publications"] = dataset.Publications
	}

	if dataset.Publisher != nil {
		if dataset.Publisher.HRef != "" {
			updates["next.publisher.href"] = dataset.Publisher.HRef
		}

		if dataset.Publisher.Name != "" {
			updates["next.publisher.name"] = dataset.Publisher.Name
		}

		if dataset.Publisher.Type != "" {
			updates["next.publisher.type"] = dataset.Publisher.Type
		}
	}

	if dataset.QMI != nil {
		updates["next.qmi.description"] = dataset.QMI.Description
		updates["next.qmi.href"] = dataset.QMI.HRef
		updates["next.qmi.title"] = dataset.QMI.Title
	}

	if dataset.RelatedDatasets != nil {
		updates["next.related_datasets"] = dataset.RelatedDatasets
	}

	if dataset.ReleaseFrequency != "" {
		updates["next.release_frequency"] = dataset.ReleaseFrequency
	}

	if dataset.State != "" {
		updates["next.state"] = dataset.State
	} else {
		if currentState == models.PublishedState {
			updates["next.state"] = models.CreatedState
		}
	}

	if dataset.Theme != "" {
		updates["next.theme"] = dataset.Theme
	}

	if dataset.Title != "" {
		updates["next.title"] = dataset.Title
	}

	if dataset.UnitOfMeasure != "" {
		updates["next.unit_of_measure"] = dataset.UnitOfMeasure
	}

	if dataset.URI != "" {
		updates["next.uri"] = dataset.URI
	}

	if dataset.Type != "" {
		updates["next.type"] = dataset.Type
	}

	if dataset.NomisReferenceURL != "" {
		updates["next.nomis_reference_url"] = dataset.NomisReferenceURL
	}

	log.Info(ctx, "built update query for dataset resource", log.Data{"dataset_id": id, "dataset": dataset, "updates": updates})

	return updates
}

// UpdateDatasetWithAssociation updates an existing dataset document with collection data
func (m *Mongo) UpdateDatasetWithAssociation(ctx context.Context, id, state string, version *models.Version) (err error) {

	update := bson.M{
		"$set": bson.M{
			"next.state":                     state,
			"next.collection_id":             version.CollectionID,
			"next.links.latest_version.href": version.Links.Version.HRef,
			"next.links.latest_version.id":   version.Links.Version.ID,
			"next.last_updated":              time.Now(),
		},
	}

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).Must().UpdateById(ctx, id, update); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrDatasetNotFound
		}
		return err
	}

	return nil
}

// UpdateVersion updates an existing version document
func (m *Mongo) UpdateVersion(ctx context.Context, currentVersion *models.Version, versionUpdate *models.Version, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from adding the event
	newETag, err = newETagForVersionUpdate(currentVersion, versionUpdate)
	if err != nil {
		return "", err
	}

	sel := selector(currentVersion.ID, bsonprim.Timestamp{}, eTagSelector)
	updates := createVersionUpdateQuery(versionUpdate, newETag)

	if _, err := m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Must().Update(ctx, sel, bson.M{"$set": updates, "$setOnInsert": bson.M{"last_updated": time.Now()}}); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrDatasetNotFound
		}
		return "", err
	}

	return newETag, nil
}

func createVersionUpdateQuery(version *models.Version, newETag string) bson.M {

	setUpdates := make(bson.M)

	/*
		Where updating a version to detached state:
		1.) explicitly set version number to nil
		2.) remove collectionID
	*/
	if version.State == models.DetachedState {
		setUpdates["collection_id"] = nil
		setUpdates["version"] = nil
	} else {
		if version.CollectionID != "" {
			setUpdates["collection_id"] = version.CollectionID
		}
	}

	if version.Alerts != nil {
		setUpdates["alerts"] = version.Alerts
	}

	if version.Downloads != nil {
		setUpdates["downloads"] = version.Downloads
	}

	if version.LatestChanges != nil {
		setUpdates["latest_changes"] = version.LatestChanges
	}

	if version.Links != nil {
		if version.Links.Spatial != nil {
			if version.Links.Spatial.HRef != "" {
				setUpdates["links.spatial.href"] = version.Links.Spatial.HRef
			}
		}
	}

	if version.ReleaseDate != "" {
		setUpdates["release_date"] = version.ReleaseDate
	}

	if version.State != "" {
		setUpdates["state"] = version.State
	}

	if version.Temporal != nil {
		setUpdates["temporal"] = version.Temporal
	}

	if version.UsageNotes != nil {
		setUpdates["usage_notes"] = version.UsageNotes
	}

	if newETag != "" {
		setUpdates["e_tag"] = newETag
	}

	return setUpdates
}

// UpsertDataset adds or overrides an existing dataset document
func (m *Mongo) UpsertDataset(ctx context.Context, id string, datasetDoc *models.DatasetUpdate) (err error) {

	update := bson.M{
		"$set": datasetDoc,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).UpsertById(ctx, id, update)

	return err
}

func (m *Mongo) RemoveDatasetVersionAndEditionLinks(ctx context.Context, id string) error {

	update := bson.M{
		"$unset": bson.M{
			"next.links.editions":       "",
			"next.links.latest_version": "",
		},
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	if _, err := m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).Must().UpdateById(ctx, id, update); err != nil {
		return fmt.Errorf("failed in query to MongoDB: %w", err)
	}

	return nil
}

// UpsertEdition adds or overrides an existing edition document
func (m *Mongo) UpsertEdition(ctx context.Context, datasetID, edition string, editionDoc *models.EditionUpdate) (err error) {

	selector := bson.M{
		"next.edition":          edition,
		"next.links.dataset.id": datasetID,
	}

	editionDoc.Next.LastUpdated = time.Now()

	update := bson.M{
		"$set": editionDoc,
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.EditionsCollection)).Upsert(ctx, selector, update)

	return err
}

// UpsertVersion adds or overrides an existing version document
func (m *Mongo) UpsertVersion(ctx context.Context, id string, version *models.Version) (err error) {

	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.InstanceCollection)).Upsert(ctx, bson.M{"id": id}, update)

	return err
}

// UpsertContact adds or overrides an existing contact document
func (m *Mongo) UpsertContact(ctx context.Context, id string, update interface{}) (err error) {

	_, err = m.Connection.Collection(m.ActualCollectionName(config.ContactsCollection)).UpsertById(ctx, id, update)

	return err
}

// CheckDatasetExists checks that the dataset exists
func (m *Mongo) CheckDatasetExists(ctx context.Context, id, state string) error {

	query := bson.M{"_id": id}
	if state != "" {
		query["current.state"] = state
	}

	var d models.Dataset
	if err := m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).FindOne(ctx, query, &d, mongodriver.Projection(bson.M{"_id": 1})); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrDatasetNotFound
		}
		return err
	}

	return nil
}

// CheckEditionExists checks that the edition of a dataset exists
func (m *Mongo) CheckEditionExists(ctx context.Context, id, editionID, state string) error {

	var query bson.M
	if state == "" {
		query = bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
		}
	} else {
		query = bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}
	}

	var d models.Edition
	if err := m.Connection.Collection(m.ActualCollectionName(config.EditionsCollection)).FindOne(ctx, query, &d, mongodriver.Projection(bson.M{"_id": 1})); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrEditionNotFound
		}
		return err
	}

	return nil
}

// DeleteDataset deletes an existing dataset document
func (m *Mongo) DeleteDataset(ctx context.Context, id string) (err error) {

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).Must().DeleteById(ctx, id); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrDatasetNotFound
		}
		return err
	}

	return nil
}

// DeleteEdition deletes an existing edition document
func (m *Mongo) DeleteEdition(ctx context.Context, id string) (err error) {

	if _, err = m.Connection.Collection(m.ActualCollectionName(config.EditionsCollection)).Must().Delete(ctx, bson.D{{Key: "id", Value: id}}); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrEditionNotFound
		}
		return err
	}

	log.Info(context.TODO(), "edition deleted", log.Data{"id": id})
	return nil
}
