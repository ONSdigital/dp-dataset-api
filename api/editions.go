package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

//This function returns a list of editions, the total count of editions that match the query parameters and an error
func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request, limit int, offset int) (interface{}, int, error) {
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

	if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
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

	if authorised {
		log.Info(ctx, "getEditions endpoint: get all edition with auth", logData)
		return results, totalCount, nil
	}

	var publicResults []*models.Edition
	for i := range results {
		publicResults = append(publicResults, results[i].Current)
	}
	log.Info(ctx, "getEditions endpoint: get all edition without auth", logData)
	return publicResults, totalCount, nil
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

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.Error(ctx, "getEdition endpoint: unable to find dataset", err, logData)
			return nil, err
		}

		edition, err := api.dataStore.Backend.GetEdition(datasetID, edition, state)
		if err != nil {
			log.Error(ctx, "getEdition endpoint: unable to find edition", err, logData)
			return nil, err
		}

		var b []byte

		if authorised {
			// User has valid authentication to get raw edition document
			b, err = json.Marshal(edition)
			if err != nil {
				log.Error(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", err, logData)
				return nil, err
			}
			log.Info(ctx, "getEdition endpoint: get edition with auth", logData)
		} else {

			// User is not authenticated and hence has only access to current sub document
			b, err = json.Marshal(edition.Current)
			if err != nil {
				log.Error(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", err, logData)
				return nil, err
			}
			log.Info(ctx, "getEdition endpoint: get edition without auth", logData)
		}
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
