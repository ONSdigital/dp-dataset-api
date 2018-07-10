package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	logData := log.Data{"dataset_id": id}
	auditParams := common.Params{"dataset_id": id}

	if auditErr := api.auditor.Record(r.Context(), getEditionsAction, audit.Attempted, auditParams); auditErr != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	b, err := func() ([]byte, error) {
		authorised, logData := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		logData["state"] = state

		if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getEditions endpoint: unable to find dataset"), logData)
			return nil, err
		}

		results, err := api.dataStore.Backend.GetEditions(id, state)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getEditions endpoint: unable to find editions for dataset"), logData)
			return nil, err
		}

		var editionBytes []byte

		if authorised {

			// User has valid authentication to get raw edition document
			editionBytes, err = json.Marshal(results)
			if err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "getEditions endpoint: failed to marshal a list of edition resources into bytes"), logData)
				return nil, err
			}
			log.InfoCtx(ctx, "getEditions endpoint: get all edition with auth", logData)

		} else {
			// User is not authenticated and hence has only access to current sub document
			var publicResults []*models.Edition
			for i := range results.Items {
				publicResults = append(publicResults, results.Items[i].Current)
			}

			editionBytes, err = json.Marshal(&models.EditionResults{Items: publicResults})
			if err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "getEditions endpoint: failed to marshal a list of edition resources into bytes"), logData)
				return nil, err
			}
			log.InfoCtx(ctx, "getEditions endpoint: get all edition without auth", logData)
		}
		return editionBytes, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getEditionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return
	}

	if auditErr := api.auditor.Record(r.Context(), getEditionsAction, audit.Successful, auditParams); auditErr != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "getEditions endpoint: failed writing bytes to response"), logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
	}
	log.InfoCtx(ctx, "getEditions endpoint: request successful", logData)
}

func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	auditParams := common.Params{"dataset_id": id, "edition": editionID}
	logData := audit.ToLogData(auditParams)

	if auditErr := api.auditor.Record(r.Context(), getEditionAction, audit.Attempted, auditParams); auditErr != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	b, err := func() ([]byte, error) {
		authorised, logData := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getEdition endpoint: unable to find dataset"), logData)
			return nil, err
		}

		edition, err := api.dataStore.Backend.GetEdition(id, editionID, state)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getEdition endpoint: unable to find edition"), logData)
			return nil, err
		}

		var b []byte

		if authorised {
			// User has valid authentication to get raw edition document
			b, err = json.Marshal(edition)
			if err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "getEdition endpoint: failed to marshal edition resource into bytes"), logData)
				return nil, err
			}
			log.InfoCtx(ctx, "getEdition endpoint: get edition with auth", logData)
		} else {

			// User is not authenticated and hence has only access to current sub document
			b, err = json.Marshal(edition.Current)
			if err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "getEdition endpoint: failed to marshal edition resource into bytes"), logData)
				return nil, err
			}
			log.InfoCtx(ctx, "getEdition endpoint: get edition without auth", logData)
		}
		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getEditionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return
	}

	if auditErr := api.auditor.Record(ctx, getEditionAction, audit.Successful, auditParams); auditErr != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "getEdition endpoint: failed to write byte to response"), logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}
	log.InfoCtx(ctx, "getEdition endpoint: request successful", logData)
}
