package api

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dataset-api/models"
	"net/http"
)

func (api *DatasetAPI) getCensus(writer http.ResponseWriter, request *http.Request) {
	blobs, err := models.NewPopulationTypes(request.Context(), api.dataStore.Backend)
	if err != nil {
		panic("not implemented")
	}

	serializedBlobs, err := json.Marshal(blobs)
	if err != nil {
		panic("not implemented")
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(serializedBlobs)
}
