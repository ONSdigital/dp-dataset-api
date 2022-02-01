package api

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dataset-api/contract"
	"net/http"
)

// GetPopulationTypesHandler is the handler for the "/population-types" endpoint
func (api *DatasetAPI) GetPopulationTypesHandler(w http.ResponseWriter, req *http.Request) {
	populationTypes, err := api.cantabularClient.ListDatasets(req.Context())
	if err != nil {
		api.logger.Error(req.Context(), "error retrieving datasets from cantabular", err, nil)
		http.Error(w, "failed to fetch population types", http.StatusInternalServerError)
		return
	}

	model := contract.PopulationTypes{}
	for _, item := range populationTypes {
		model.Items = append(model.Items, contract.PopulationType{
			Name: item,
		})
	}

	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(model)
	if err != nil {
		api.logger.Error(req.Context(), "failed to encode and write population types model to response object", err, nil)
		http.Error(w, "failed to respond with population types", http.StatusInternalServerError)
		return
	}
}
