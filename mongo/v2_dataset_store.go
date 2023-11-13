package mongo

import (
	"context"
	"time"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"go.mongodb.org/mongo-driver/bson"
)

// UpsertLDDataset adds or overrides an existing dataset document
func (m *Mongo) UpsertLDDataset(ctx context.Context, id string, datasetDoc *models.LDDataset) (err error) {
	update := bson.M{
		"$set": datasetDoc,
		// "$setOnInsert": bson.M{
		// 	"last_updated": time.Now(),
		// },
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.V2DatasetsCollection)).UpsertById(ctx, id, update)

	return err
}

// UpsertLDDataset adds or overrides an existing dataset document
func (m *Mongo) UpsertLDInstance(ctx context.Context, id string, instanceDoc *models.LDInstance) (err error) {
	// if instanceDoc != nil {
	// 	instanceDoc.DCATDataset.DCATDatasetSeries = models.DCATDatasetSeries{}
	// }

	update := bson.M{
		"$set": instanceDoc,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).UpsertById(ctx, id, update)

	return err
}
