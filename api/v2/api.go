package v2

import (
	"github.com/ONSdigital/dp-dataset-api/api/common"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
)

type DatasetAPI struct {
	dataStore          store.DataStore
	downloadGenerators map[models.DatasetType]common.DownloadsGenerator
	datasetPermissions common.AuthHandler
	permissions        common.AuthHandler
}

func NewDatasetAPI(dataStore store.DataStore, downloadGenerators map[models.DatasetType]common.DownloadsGenerator, datasetPermissions common.AuthHandler, permissions common.AuthHandler) *DatasetAPI {
	return &DatasetAPI{
		dataStore:          dataStore,
		downloadGenerators: downloadGenerators,
		datasetPermissions: datasetPermissions,
		permissions:        permissions,
	}
}
