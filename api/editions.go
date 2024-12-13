package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	"github.com/ONSdigital/dp-net/v2/links"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

// This function returns a list of editions, the total count of editions that match the query parameters and an error
// TODO: Refactor this to have named results
//
//nolint:gocritic // Naming results requires some refactoring here.
func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}

	authorised := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	logData["state"] = state

	if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
		log.Error(ctx, "getEditions endpoint: unable to find dataset", err, logData)
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return nil, 0, err
	}

	results, totalCount, err := api.dataStore.Backend.GetEditions(ctx, datasetID, state, offset, limit, authorised)
	if err != nil {
		log.Error(ctx, "getEditions endpoint: unable to find editions for dataset", err, logData)
		if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return nil, 0, err
	}

	linksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetWebsiteURL())
	editionsResponse, err := utils.MapEditionsAndRewriteLinks(ctx, results, authorised, linksBuilder)
	if err != nil {
		log.Error(ctx, "Error mapping results and rewriting links", err)
		return nil, 0, err
	}

	return editionsResponse, totalCount, nil
}

func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}

	b, err := func() ([]byte, error) {
		authorised := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
			log.Error(ctx, "getEdition endpoint: unable to find dataset", err, logData)
			return nil, err
		}

		edition, err := api.dataStore.Backend.GetEdition(ctx, datasetID, edition, state)
		if err != nil {
			log.Error(ctx, "getEdition endpoint: unable to find edition", err, logData)
			return nil, err
		}

		linksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetWebsiteURL())
		editionResponse, err := utils.MapEditionsAndRewriteLinks(ctx, []*models.EditionUpdate{edition}, authorised, linksBuilder)
		if err != nil {
			log.Error(ctx, "Error mapping results and rewriting links", err)
			return nil, err
		}

		var b []byte
		b, err = json.Marshal(editionResponse)
		if err != nil {
			log.Error(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", err, logData)
			return nil, err
		}
		log.Info(ctx, "getEdition endpoint: get edition", logData)
		return b, nil
	}()

	if err != nil {
		if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(ctx, "getEdition endpoint: failed to write byte to response", err, logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}
	log.Info(ctx, "getEdition endpoint: request successful", logData)
}
