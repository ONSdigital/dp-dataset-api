package api

import (
	"github.com/ONSdigital/dp-dataset-api/models"
)

// DataStore represents an interface used to store datasets
type DataStore interface {
	GetFilter() (models.Dataset, error)
}
