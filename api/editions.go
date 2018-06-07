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

	b, taskErr := func() ([]byte, *httpError) {
		authorised, logData := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		logData["state"] = state
		log.Info("about to check resources exist", logData)

		if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
			audit.LogError(ctx, errors.WithMessage(err, "getEditions endpoint: unable to find dataset"), logData)
			return nil, &httpError{errs.ErrDatasetNotFound, http.StatusNotFound}
		}

		results, err := api.dataStore.Backend.GetEditions(id, state)
		if err != nil {
			audit.LogError(ctx, errors.WithMessage(err, "getEditions endpoint: unable to find editions for dataset"), logData)
			return nil, &httpError{errs.ErrEditionNotFound, http.StatusNotFound}
		}

		var editionBytes []byte

		if authorised {

			// User has valid authentication to get raw edition document
			editionBytes, err = json.Marshal(results)
			if err != nil {
				audit.LogError(ctx, errors.WithMessage(err, "getEditions endpoint: failed to marshal a list of edition resources into bytes"), logData)
				return nil, &httpError{err, http.StatusInternalServerError}
			}
			audit.LogInfo(ctx, "getEditions endpoint: get all edition with auth", logData)

		} else {
			// User is not authenticated and hence has only access to current sub document
			var publicResults []*models.Edition
			for i := range results.Items {
				publicResults = append(publicResults, results.Items[i].Current)
			}

			editionBytes, err = json.Marshal(&models.EditionResults{Items: publicResults})
			if err != nil {
				audit.LogError(ctx, errors.WithMessage(err, "getEditions endpoint: failed to marshal a list of edition resources into bytes"), logData)
				return nil, &httpError{err, http.StatusInternalServerError}
			}
			audit.LogInfo(ctx, "getEditions endpoint: get all edition without auth", logData)
		}
		return editionBytes, nil
	}()

	if taskErr != nil {
		if auditErr := api.auditor.Record(ctx, getEditionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			taskErr = &httpError{auditErr, http.StatusInternalServerError}
		}
		http.Error(w, taskErr.Error(), taskErr.status)
		return
	}

	if auditErr := api.auditor.Record(r.Context(), getEditionsAction, audit.Successful, auditParams); auditErr != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err := w.Write(b)
	if err != nil {
		audit.LogError(ctx, errors.WithMessage(err, "getEditions endpoint: failed writing bytes to response"), logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
	}
	audit.LogInfo(ctx, "getEditions endpoint: request successful", logData)
}

func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	logData := log.Data{"dataset_id": id, "edition": editionID}
	auditParams := common.Params{"dataset_id": id, "edition": editionID}

	if auditErr := api.auditor.Record(r.Context(), getEditionAction, audit.Attempted, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("unable to find dataset", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getEditionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(editionDocType, err, w)
		return
	}

	edition, err := api.dataStore.Backend.GetEdition(id, editionID, state)
	if err != nil {
		log.ErrorC("unable to find edition", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getEditionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(editionDocType, err, w)
		return
	}

	var logMessage string
	var b []byte

	if authorised {

		// User has valid authentication to get raw edition document
		b, err = json.Marshal(edition)
		if err != nil {
			log.ErrorC("failed to marshal edition resource into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logMessage = "get edition with auth"

	} else {

		// User is not authenticated and hance has only access to current sub document
		b, err = json.Marshal(edition.Current)
		if err != nil {
			log.ErrorC("failed to marshal public edition resource into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logMessage = "get public edition without auth"
	}

	if auditErr := api.auditor.Record(r.Context(), getEditionAction, audit.Successful, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug(logMessage, logData)
}
