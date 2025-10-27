package mongo

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"

	"go.mongodb.org/mongo-driver/bson"
)

// AcquireVersionsLock tries to lock the provided versionID.
func (m *Mongo) AcquireVersionsLock(ctx context.Context, versionID string) (lockID string, err error) {
	return m.lockClientVersionsCollection.Acquire(ctx, versionID)
}

func (m *Mongo) UnlockVersions(ctx context.Context, lockID string) {
	m.lockClientVersionsCollection.Unlock(ctx, lockID)
}

// UpsertVersion adds or overrides an existing version document
func (m *Mongo) UpsertVersionStatic(ctx context.Context, id string, version *models.Version) (err error) {
	version.LastUpdated = time.Now()
	update := bson.M{
		"$set": version,
	}

	sel := bson.M{
		"edition": version.Edition,
		"version": version.Version,
		"e_tag":   version.ETag,
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).UpsertOne(ctx, sel, update)
	return err
}

// AddVersion to the versions collection
func (m *Mongo) AddVersionStatic(ctx context.Context, version *models.Version) (inst *models.Version, err error) {
	version.LastUpdated = time.Now().UTC()
	timestamp := version.LastUpdated.Unix()
	if timestamp < 0 || timestamp > int64(^uint32(0)) {
		return nil, errors.New("timestamp out of range for uint32")
	}

	// set eTag value to current hash of the version
	version.ETag, err = version.Hash(nil)
	if err != nil {
		return nil, err
	}

	// Insert version to database
	if _, err = m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Insert(ctx, &version); err != nil {
		return nil, err
	}

	return version, nil
}

// CheckEditionExistsStatic checks that the edition of a dataset exists in the versions collection
func (m *Mongo) CheckEditionExistsStatic(ctx context.Context, datasetID, editionID, state string) error {
	query := bson.M{
		"links.dataset.id": datasetID,
		"links.edition.id": editionID,
	}
	if state != "" {
		query["state"] = state
	}

	var d models.Version
	if err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).FindOne(ctx, query, &d, mongodriver.Projection(bson.M{"_id": 1})); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrEditionNotFound
		}
		return err
	}

	return nil
}

// CheckVersionExistsStatic checks that the version of a dataset exists in the versions collection
func (m *Mongo) CheckVersionExistsStatic(ctx context.Context, datasetID, editionID string, version int) (bool, error) {
	query := bson.M{
		"links.dataset.id": datasetID,
		"links.edition.id": editionID,
		"version":          version,
	}

	count, err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Count(ctx, query)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// GetStaticVersionsByState retrieves all versions that match the provided state
// If state is empty, the search will include any state that is not "published"
func (m *Mongo) GetStaticVersionsByState(ctx context.Context, state, publishedOnly string, offset, limit int) ([]*models.Version, int, error) {
	filter := bson.M{"type": models.Static.String()}

	if state != "" {
		filter["state"] = state
	}

	if publishedOnly != "" {
		val, _ := strconv.ParseBool(strings.ToLower(publishedOnly))
		if val {
			filter["state"] = models.PublishedState
		} else {
			filter["state"] = bson.M{"$ne": models.PublishedState}
		}
	}

	results := []*models.Version{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Find(ctx, filter, &results,
		mongodriver.Sort(bson.M{"last_updated": -1}),
		mongodriver.Offset(offset),
		mongodriver.Limit(limit))
	if err != nil {
		return results, 0, err
	}

	if totalCount == 0 {
		return nil, 0, errs.ErrVersionsNotFound
	}

	return results, totalCount, nil
}

// GetVersions retrieves all version documents for a dataset
func (m *Mongo) GetVersionsStatic(ctx context.Context, datasetID, edition, state string, offset, limit int) ([]models.Version, int, error) {
	selector := buildVersionsQuery(datasetID, edition, state)
	// get total count and paginated values according to provided offset and limit
	results := []models.Version{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Find(ctx, selector, &results,
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

// GetVersion retrieves a version document for a dataset edition
func (m *Mongo) GetVersionStatic(ctx context.Context, id, editionID string, versionID int, state string) (*models.Version, error) {
	selector := buildVersionQuery(id, editionID, state, versionID)

	var version models.Version
	err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).FindOne(ctx, selector, &version)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrVersionNotFound
		}
		return nil, err
	}

	return &version, nil
}

// GetLatestVersionStatic retrieves the latest version for an edition of a dataset
func (m *Mongo) GetLatestVersionStatic(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
	selector := bson.M{
		"links.dataset.id": datasetID,
		"links.edition.id": editionID,
	}
	if state != "" {
		selector["state"] = state
	}

	var version models.Version
	err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).FindOne(ctx, selector, &version, mongodriver.Sort(bson.M{"version": -1}))
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrVersionNotFound
		}
		return nil, err
	}

	return &version, nil
}

// GetDatasetType retrieves the type of a dataset
func (m *Mongo) GetDatasetType(ctx context.Context, datasetID string, authorised bool) (string, error) {
	selector := bson.M{
		"_id": datasetID,
	}

	if !authorised {
		selector["current.state"] = models.PublishedState
	}

	var d models.DatasetUpdate

	if authorised {
		if err := m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).FindOne(ctx, selector, &d, mongodriver.Projection(bson.M{"next.type": 1})); err != nil {
			if errors.Is(err, mongodriver.ErrNoDocumentFound) {
				return "", errs.ErrDatasetNotFound
			}
			return "", err
		}
		return d.Next.Type, nil
	}

	if err := m.Connection.Collection(m.ActualCollectionName(config.DatasetsCollection)).FindOne(ctx, selector, &d, mongodriver.Projection(bson.M{"current.type": 1})); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrDatasetNotFound
		}
		return "", err
	}
	return d.Current.Type, nil
}

// UpdateVersionStatic updates an existing version document
func (m *Mongo) UpdateVersionStatic(ctx context.Context, currentVersion, versionUpdate *models.Version, eTagSelector string) (newETag string, err error) {
	// calculate the new eTag hash for the instance that would result from adding the event
	newETag, err = newETagForVersionUpdate(currentVersion, versionUpdate)
	if err != nil {
		return "", err
	}

	sel := bson.M{
		"edition": currentVersion.Edition,
		"version": currentVersion.Version,
		"e_tag":   eTagSelector,
	}
	updates := createVersionUpdateQuery(versionUpdate, newETag)

	if _, err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Must().Update(ctx, sel, bson.M{"$set": updates}); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return "", errs.ErrVersionNotFound
		}
		return "", err
	}

	return newETag, nil
}

func (m *Mongo) GetAllStaticVersions(ctx context.Context, datasetID, state string, offset, limit int) ([]*models.Version, int, error) {
	selector := bson.M{"links.dataset.id": datasetID}
	if state != "" {
		selector["state"] = state
	}
	// get total count and paginated values according to provided offset and limit
	results := []*models.Version{}
	totalCount, err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Find(ctx, selector, &results,
		mongodriver.Sort(bson.M{"last_updated": -1}),
		mongodriver.Offset(offset),
		mongodriver.Limit(limit))
	if err != nil {
		return results, 0, err
	}

	if totalCount < 1 {
		return nil, 0, errs.ErrVersionNotFound
	}

	return results, totalCount, nil
}

func (m *Mongo) DeleteStaticDatasetVersion(ctx context.Context, datasetID, editionID, versionNumber string) (err error) {
	// convert versionNumber to int
	versionInt, _ := strconv.Atoi(versionNumber)
	// proceed to delete version
	filter := bson.M{
		"links.dataset.id": datasetID,
		"edition":          editionID,
		"version":          versionInt,
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Must().Delete(ctx, filter)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return errs.ErrVersionNotFound
		}
		return err
	}

	return nil
}
