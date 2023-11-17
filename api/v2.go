package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/liip/sheriff"
)

func marshal(data interface{}, opts ...string) ([]byte, error) {
	o := &sheriff.Options{
		Groups:          []string{"all"},
		IncludeEmptyTag: true,
	}

	if len(opts) > 0 {
		o.Groups = append(o.Groups, opts...)
	}

	data, err := sheriff.Marshal(o, data)
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

func (api *DatasetAPI) getV2Datasets(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)
	ctx := r.Context()

	b, err := func() ([]byte, error) {
		list, _, err := api.dataStore.Backend.GetV2Datasets(ctx, 0, 100, true)
		if err != nil {
			log.Error(ctx, "getV2Datasets endpoint: datastore.getV2Datasets returned an error", err)
			return nil, err
		}

		//linked data fields - @id per item
		for i, l := range list {
			list[i].ID = fmt.Sprintf("%s/v2/datasets/%s", api.host, l.Identifier)
		}

		// TODO: fix pagination
		page := &models.DatasetList{
			Items: list,
			Page: models.Page{
				TotalCount: len(list),
			},
			LinkedData: models.LinkedData{
				Context: "cdn.ons.gov.uk/context.json",
			},
			Links: &models.PageLinks{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets", api.host),
				},
				Next: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets", api.host),
				},
				Prev: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets", api.host),
				},
			},
		}

		groups := []string{"datasets"}
		b, err := marshal(page, groups...)
		if err != nil {
			log.Error(ctx, "getV2Datasets endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Datasets endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Datasets endpoint: request successful")
}

func (api *DatasetAPI) getV2Dataset(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetV2Dataset(ctx, datasetID, true)
		if err != nil {
			log.Error(ctx, "getV2Dataset endpoint: datastore.getV2Dataset returned an error", err)
			return nil, err
		}

		dataset.LinkedData = models.LinkedData{
			Context: "cdn.ons.gov.uk/context.json",
			ID:      fmt.Sprintf("%s/v2/datasets/%s", api.host, dataset.Identifier),
			Type:    []string{"dcat:datasetSeries"},
		}

		dataset.Links = &models.LDDatasetLinks{
			Self: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s", api.host, dataset.Identifier),
			},
			Editions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, dataset.Identifier),
			},
		}

		// replace embedded @id field with proper URLs and set latest version link
		if dataset.Embedded != nil && len(dataset.Embedded.Editions) > 0 {
			for i, ed := range dataset.Embedded.Editions {
				s := fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, dataset.Identifier, ed.ID)
				dataset.Embedded.Editions[i].ID = s
			}

			dataset.Links.LatestVersion = &models.LinkObject{
				HRef: dataset.Embedded.Editions[0].ID,
			}
		}

		// TODO set etag header?

		groups := []string{"dataset"}
		b, err := marshal(dataset, groups...)
		if err != nil {
			log.Error(ctx, "getV2Dataset endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Dataset endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Dataset endpoint: request successful")
}
