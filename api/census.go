package api

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dataset-api/models"
	"net/http"
)

func (api *DatasetAPI) getCensus(writer http.ResponseWriter, request *http.Request) {
	populationTypes, err := models.FetchPopulationTypes(request.Context(), api.dataStore.Backend)
	if err != nil {
		http.Error(writer, "failed to fetch population types", http.StatusInternalServerError)
		return
	}

	writer.Header().Set("content-type", "application/json")
	err = json.NewEncoder(writer).Encode(populationTypes)
	if err != nil {
		http.Error(writer, "failed to respond with population types", http.StatusInternalServerError)
		return
	}
}
