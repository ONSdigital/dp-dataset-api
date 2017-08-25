package api

import "github.com/ONSdigital/dp-dataset-api/datastore"

// DataStore provides a datastore.Backend interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend datastore.Backend
}
