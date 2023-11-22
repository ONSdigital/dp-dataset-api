package api

import (
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) getV2Instances(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)
	ctx := r.Context()

	stateFilterQuery := r.URL.Query().Get("state")
	datasetFilterQuery := r.URL.Query().Get("dataset")

	b, err := func() ([]byte, error) {
		list, _, err := api.dataStore.Backend.GetV2Instances(ctx, datasetFilterQuery, stateFilterQuery, 0, 100)
		if err != nil {
			log.Error(ctx, "getV2Instances endpoint: datastore.getV2Instances returned an error", err)
			return nil, err
		}

		//linked data fields - @id per item
		for i, l := range list {
			list[i].ID = fmt.Sprintf("%s/v2/instances/%s", api.host, l.InstanceID)
		}

		// TODO: fix pagination
		page := &models.InstanceList{
			Items: list,
			Page: models.Page{
				TotalCount: len(list),
			},
			Links: &models.PageLinks{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/instances", api.host),
				},
				Next: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/instances", api.host),
				},
				Prev: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/instances", api.host),
				},
			},
		}

		groups := []string{"instances"}
		b, err := marshal(page, groups...)
		if err != nil {
			log.Error(ctx, "getV2Instances endpoint: marshal returned an error", err)
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
		log.Error(ctx, "getV2Instances endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Instances endpoint: request successful")
}

func (api *DatasetAPI) getV2Instance(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]

	b, err := func() ([]byte, error) {
		inst, err := api.dataStore.Backend.GetV2Instance(ctx, instanceID)
		if err != nil {
			log.Error(ctx, "getV2Instance endpoint: datastore.getV2Instance returned an error", err)
			return nil, err
		}

		// we're storing the IDs but we need to build the HRefs
		if inst.Links != nil && inst.Links.Dataset != nil {
			dataset := inst.Links.Dataset.ID
			edition := inst.Links.Edition.ID

			inst.Links.Dataset.HRef = fmt.Sprintf("%s/v2/datasets/%s", api.host, dataset)
			inst.Links.Edition.HRef = fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, dataset, edition)

			if inst.Links.Version != nil {
				inst.Links.Version.HRef = fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%s", api.host, dataset, edition, inst.Links.Version.ID)
			}
		}

		// TODO set etag header?

		groups := []string{"instance"}
		b, err := marshal(inst, groups...)
		if err != nil {
			log.Error(ctx, "getV2Instance endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Instance endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Instance endpoint: request successful")
}
