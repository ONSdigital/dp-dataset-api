package api

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dataset-api/models"
	"net/http"
)

// this is the handler for the "/census" endpoint
func (api *DatasetAPI) getPopulationTypes(w http.ResponseWriter, req *http.Request) {
	populationTypes, err := models.FetchPopulationTypes(req.Context(), api.dataStore.Backend)
	if err != nil {
		http.Error(w, "failed to fetch population types", http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(populationTypes)
	if err != nil {
		http.Error(w, "failed to respond with population types", http.StatusInternalServerError)
		return
	}
}
