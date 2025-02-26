package mongo

import (
	"context"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

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
