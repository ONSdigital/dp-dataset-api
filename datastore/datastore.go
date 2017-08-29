package datastore

import "github.com/ONSdigital/dp-dataset-api/models"

// Backend represents basic data access via Get, Remove and Upsert methods.
type Backend interface {
	GetDatasets() (*models.DatasetResults, error)
	UpsertDataset(id interface{}, update interface{}) error
	UpsertEdition(id interface{}, update interface{}) error
	UpsertVersion(id interface{}, update interface{}) error
	UpsertContact(id interface{}, update interface{}) error

}
