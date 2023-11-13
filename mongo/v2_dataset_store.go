package mongo

import (
	"context"
	"errors"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"
	"go.mongodb.org/mongo-driver/bson"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
)

// GetV2Datasets retrieves all dataset documents
func (m *Mongo) GetV2Datasets(ctx context.Context, offset, limit int, authorised bool) (values []*models.LDDataset, totalCount int, err error) {
	var filter bson.M
	if !authorised {
		filter = bson.M{"state": bson.M{"$eq": "published"}}
	}

	values = []*models.LDDataset{}
	log.Info(ctx, "getV2Datasets mongo details", log.Data{"collection": config.V2DatasetsCollection, "filter": filter})
	totalCount, err = m.Connection.Collection(m.ActualCollectionName(config.V2DatasetsCollection)).Find(ctx, filter, &values,
		mongodriver.Sort(bson.M{"_id": -1}), mongodriver.Offset(offset), mongodriver.Limit(limit))
	if err != nil {
		return nil, 0, err
	}

	return values, totalCount, nil
}

// GetV2Dataset retrieves a dataset document
func (m *Mongo) GetV2Dataset(ctx context.Context, authorised bool, id string) (*models.LDDataset, error) {
	var filter bson.M
	if !authorised {
		filter = bson.M{"_id": id, "state": bson.M{"$eq": "published"}}
	} else {
		filter = bson.M{"_id": id}
	}

	var dataset models.LDDataset
	err := m.Connection.Collection(m.ActualCollectionName(config.V2DatasetsCollection)).FindOne(ctx, filter, &dataset)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}
	log.Info(ctx, "getV2Dataset mongo result", log.Data{"dataset": dataset})
	return &dataset, nil
}

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
