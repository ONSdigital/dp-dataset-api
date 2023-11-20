package mongo

import (
	"context"
	"errors"

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
func (m *Mongo) GetV2Dataset(ctx context.Context, id string, authorised bool) (*models.LDDataset, error) {
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

	editions, err := m.getV2DatasetEmbeds(ctx, id, "published", authorised)
	if err != nil && !errors.Is(err, mongodriver.ErrNoDocumentFound) {
		return nil, err
	}
	dataset.Embedded = editions

	return &dataset, nil
}

// getV2DatasetEmbeds queries editions documents to find the necessary fields to embed for a dataset
func (m *Mongo) getV2DatasetEmbeds(ctx context.Context, id, state string, authorised bool) (*models.DatasetEmbedded, error) {
	stages := buildDatasetEmbeddedQuery(id, state, authorised)
	stages = append(stages, bson.M{
		"$sort": bson.M{"issued": -1},
	})

	var result []models.EmbeddedEdition
	if err := m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).Aggregate(ctx, stages, &result); err != nil {
		return nil, err
	}

	return &models.DatasetEmbedded{
		Editions: result,
	}, nil
}

// GetEditions retrieves all edition documents for a dataset
func (m *Mongo) GetV2Editions(ctx context.Context, id, state string, offset, limit int, authorised bool) ([]*models.LDEdition, int, error) {
	stages := buildLatestEditionAndVersionQuery(id, "", 0, state, authorised)
	stages = append(stages,
		bson.M{"$sort": bson.M{"_id": 1}},
		bson.M{"$limit": limit},
		bson.M{"$skip": offset},
	)

	// get total count and paginated values according to provided offset and limit
	results := []*models.LDEdition{}
	if err := m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).Aggregate(ctx, stages, &results); err != nil {
		return results, 0, err
	}

	dataset, err := m.getDCATDatasetSeries(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	for i := range results {
		results[i].DCATDatasetSeries = *dataset
	}

	return results, len(results), nil
}

// GetEdition retrieves an edition document for a dataset
func (m *Mongo) GetV2Edition(ctx context.Context, id, editionID, state string, authorised bool) (*models.LDEdition, error) {
	stages := buildLatestEditionAndVersionQuery(id, editionID, 0, state, authorised)
	// get the latest version for this edition
	stages = append(stages,
		bson.M{"$sort": bson.M{"version": -1}},
		bson.M{"$limit": 1},
	)

	var result []models.LDEdition
	err := m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).Aggregate(ctx, stages, &result)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrEditionNotFound
		}
		return nil, err
	}

	if len(result) != 1 {
		return nil, errors.New("invalid number of editions returned from mongo query")
	}

	edition := result[0]

	dataset, err := m.getDCATDatasetSeries(ctx, id)
	if err != nil {
		return nil, err
	}
	edition.DCATDatasetSeries = *dataset

	embeds, err := m.getV2EditionEmbeds(ctx, id, editionID, state, authorised)
	if err != nil {
		return nil, err
	}
	edition.Embedded = embeds

	return &edition, nil
}

// getV2EditionEmbeds queries editions documents to find the necessary fields to embed for a edition
func (m *Mongo) getV2EditionEmbeds(ctx context.Context, id, edition, state string, authorised bool) (*models.EditionEmbedded, error) {
	stages := buildVersionListQuery(id, edition, state, authorised)
	stages = append(stages, bson.M{
		"$sort": bson.M{"version": -1},
	})

	var versions []models.EmbeddedVersion
	if err := m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).Aggregate(ctx, stages, &versions); err != nil {
		return nil, err
	}

	// TODO: add dimension and distribution embeds

	return &models.EditionEmbedded{
		Versions:      versions,
		Dimensions:    nil,
		Distributions: nil,
	}, nil
}

// GetV2Versions retrieves a version document for an edition
func (m *Mongo) GetV2Versions(ctx context.Context, id, editionID, state string, offset, limit int, authorised bool) ([]*models.LDEdition, int, error) {
	stages := buildVersionListQuery(id, editionID, state, authorised)
	stages = append(stages,
		bson.M{"$sort": bson.M{"version": -1}},
		bson.M{"$limit": limit},
		bson.M{"$skip": offset},
	)

	var results []*models.LDEdition
	err := m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).Aggregate(ctx, stages, &results)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, 0, errs.ErrEditionNotFound
		}
		return nil, 0, err
	}

	dataset, err := m.getDCATDatasetSeries(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	for i := range results {
		results[i].DCATDatasetSeries = *dataset
	}

	return results, len(results), nil
}

func (m *Mongo) GetV2Version(ctx context.Context, id, editionID string, versionID int, state string, authorised bool) (*models.LDEdition, error) {
	stages := buildLatestEditionAndVersionQuery(id, editionID, versionID, state, authorised)
	stages = append(stages,
		bson.M{"$sort": bson.M{"version": -1}},
		bson.M{"$limit": 1},
	)

	var result []models.LDEdition
	err := m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).Aggregate(ctx, stages, &result)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrEditionNotFound
		}
		return nil, err
	}

	if len(result) != 1 {
		return nil, errors.New("invalid number of editions returned from mongo query")
	}

	edition := result[0]
	dataset, err := m.getDCATDatasetSeries(ctx, id)
	if err != nil {
		return nil, err
	}

	edition.DCATDatasetSeries = *dataset

	return &edition, nil
}

// getDCATDatasetSeries should be used to return dataset details for other endpoints such as editions and versions
// It differs from getV2Dataset by avoiding collisions on fields such as collectionID, and omits _embedded fields
func (m *Mongo) getDCATDatasetSeries(ctx context.Context, id string) (*models.DCATDatasetSeries, error) {
	var dataset *models.DCATDatasetSeries
	err := m.Connection.Collection(m.ActualCollectionName(config.V2DatasetsCollection)).FindOne(ctx, bson.M{"_id": id}, &dataset)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrDatasetNotFound
		}
		return nil, err
	}

	return dataset, nil
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
	update := bson.M{
		"$set": instanceDoc,
		// "$setOnInsert": bson.M{
		// 	"last_updated": time.Now(),
		// },
	}

	_, err = m.Connection.Collection(m.ActualCollectionName(config.V2InstancesCollection)).UpsertById(ctx, id, update)

	return err
}
