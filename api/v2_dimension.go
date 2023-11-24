package api

import (
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) getV2Dimensions(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	editionID := vars["edition"]
	version := vars["version"]

	b, err := func() ([]byte, error) {
		versionID, err := models.ParseAndValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "getV2Dimensions endpoint: invalid version", err)
			return nil, err
		}

		//check the version can be returned for the auth/state combination before querying for dimensions
		_, err = api.dataStore.Backend.GetV2Version(ctx, datasetID, editionID, versionID, "published", true)
		if err != nil {
			log.Error(ctx, "getV2Dimensions endpoint: datastore.getV2Version returned an error", err)
			return nil, err
		}

		list, _, err := api.dataStore.Backend.GetV2Dimensions(ctx, datasetID, editionID, versionID, 0, 100)
		if err != nil {
			log.Error(ctx, "getV2Dimensions endpoint: datastore.GetV2Dimensions returned an error", err)
			return nil, err
		}

		//linked data fields - @id per item
		for i, d := range list {
			list[i].ID = fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d/dimensions/%s", api.host, datasetID, editionID, versionID, d.Name)
		}

		// TODO: fix pagination
		page := &models.DimensionList{
			Items: list,
			Page: models.Page{
				TotalCount: len(list),
			},
			LinkedData: models.LinkedData{
				Context: "cdn.ons.gov.uk/context.json",
			},
			Links: &models.PageLinks{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d/dimensions", api.host, datasetID, editionID, versionID),
				},
				Next: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d/dimensions", api.host, datasetID, editionID, versionID),
				},
				Prev: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d/dimensions", api.host, datasetID, editionID, versionID),
				},
			},
		}

		groups := []string{"dimensions"}
		b, err := marshal(page, groups...)
		if err != nil {
			log.Error(ctx, "getV2Dimensions endpoint: marshal returned an error", err, log.Data{"groups": groups})
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "getV2Versions endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Versions endpoint: request successful")
}
