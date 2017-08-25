package datastore

import "github.com/ONSdigital/dp-dataset-api/models"

// Backend represents basic data access via Get, Remove and Upsert methods.
type Backend interface {
	GetAllDatasets() (*models.DatasetResults, error)
}
