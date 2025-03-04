package mongo

import (
	"context"
	"errors"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"

	"go.mongodb.org/mongo-driver/bson"
)

// UpsertVersion adds or overrides an existing version document
func (m *Mongo) UpsertVersionStatic(ctx context.Context, id string, version *models.Version) (err error) {
	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).Upsert(ctx, bson.M{"id": id}, update)

	return err
}

// GetNextVersion retrieves the latest version for an edition of a dataset
func (m *Mongo) GetNextVersionStatic(ctx context.Context, datasetID, edition string) (int, error) {
	var version models.Version
	var nextVersion int

	selector := bson.M{
		"links.dataset.id": datasetID,
		"links.edition.id": edition,
	}

	// Results are sorted in reverse order to get latest version
	err := m.Connection.Collection(m.ActualCollectionName(config.VersionsCollection)).FindOne(ctx, selector, &version, mongodriver.Sort(bson.M{"version": -1}))
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return 1, nil
		}
		return nextVersion, err
	}

	nextVersion = version.Version + 1

	return nextVersion, nil
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

// CheckEditionExists checks that the edition of a dataset exists in the versions collection
func (m *Mongo) CheckEditionExistsStatic(ctx context.Context, id, editionID, state string) error {
	query := bson.M{
		"links.dataset.id": id,
		"links.edition.id": editionID,
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

// GetVersions retrieves all version documents for a dataset
func (m *Mongo) GetVersionsWithDatasetID(ctx context.Context, datasetID string, offset, limit int) ([]models.Version, int, error) {
	selector := buildVersionWithDatasetIDQuery(datasetID)
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

func buildVersionWithDatasetIDQuery(id string) bson.M {
	selector := bson.M{
		"links.dataset.id": id,
	}
	return selector
}
