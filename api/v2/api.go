package v2

import (
	"github.com/ONSdigital/dp-dataset-api/api/common"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

type DatasetAPI struct {
	dataStore          store.DataStore
	downloadGenerators map[models.DatasetType]common.DownloadsGenerator
}

func NewDatasetAPI(dataStore store.DataStore, downloadGenerators map[models.DatasetType]common.DownloadsGenerator) *DatasetAPI {
	return &DatasetAPI{
		dataStore:          dataStore,
		downloadGenerators: downloadGenerators,
	}
}
