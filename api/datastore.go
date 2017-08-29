package api

import "github.com/ONSdigital/dp-dataset-api/models"

// DataStore provides a datastore.Backend interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Backend
}

// Backend represents basic data access via Get, Remove and Upsert methods.
type Backend interface {
	GetAllDatasets() (*models.DatasetResults, error)
	UpsertDataset(id interface{}, update interface{}) error
	UpsertEdition(id interface{}, update interface{}) error
	UpsertVersion(id interface{}, update interface{}) error
	UpsertContact(id interface{}, update interface{}) error
}
