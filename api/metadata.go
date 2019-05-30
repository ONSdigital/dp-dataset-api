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

func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}
	logData := audit.ToLogData(auditParams)

	if auditErr := api.auditor.Record(ctx, getMetadataAction, audit.Attempted, auditParams); auditErr != nil {
		handleMetadataErr(w, auditErr)
		return
	}

	b, err := func() ([]byte, error) {

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, "")
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to find version for dataset edition"), logData)
			return nil, errs.ErrMetadataVersionNotFound
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getMetadata endpoint: get datastore.getDataset returned an error"), logData)
			return nil, err
		}

		authorised, logData := api.authenticate(r, logData)
		state := versionDoc.State

		// if the requested version is not yet published and the user is unauthorised, return a 404
		if !authorised && versionDoc.State != models.PublishedState {
			log.ErrorCtx(ctx, errors.WithMessage(errs.ErrUnauthorised, "getMetadata endpoint: unauthorised user requested unpublished version, returning 404"), logData)
			return nil, errs.ErrUnauthorised
		}

		// if request is not authenticated but the version is published, restrict dataset access to only published resources
		if !authorised {
			// Check for current sub document
			if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
				logData["dataset"] = datasetDoc.Current
				log.ErrorCtx(ctx, errors.New("getMetadata endpoint: caller not is authorised and dataset but currently unpublished"), logData)
				return nil, errs.ErrDatasetNotFound
			}

			state = datasetDoc.Current.State
		}

		if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to find edition for dataset"), logData)
			return nil, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			log.ErrorCtx(ctx, errors.WithMessage(err, "getMetadata endpoint: unpublished version has an invalid state"), logData)
			return nil, err
		}

		var metaDataDoc *models.Metadata
		// combine version and dataset metadata
		if state != models.PublishedState {
			metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Next, versionDoc, api.urlBuilder)
		} else {
			metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Current, versionDoc, api.urlBuilder)
		}

		b, err := json.Marshal(metaDataDoc)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to marshal metadata resource into bytes"), logData)
			return nil, err
		}
		return b, err
	}()

	if err != nil {
		log.ErrorCtx(ctx, err, logData)
		if auditErr := api.auditor.Record(ctx, getMetadataAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleMetadataErr(w, err)
		return
	}

	if auditErr := api.auditor.Record(ctx, getMetadataAction, audit.Successful, auditParams); auditErr != nil {
		handleMetadataErr(w, auditErr)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to write bytes to response"), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.InfoCtx(ctx, "getMetadata endpoint: get metadata request successful", logData)
}

func handleMetadataErr(w http.ResponseWriter, err error) {
	var responseStatus int

	switch {
	case err == errs.ErrUnauthorised:
		responseStatus = http.StatusNotFound
	case err == errs.ErrEditionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrMetadataVersionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrDatasetNotFound:
		responseStatus = http.StatusNotFound
	default:
		err = errs.ErrInternalServer
		responseStatus = http.StatusInternalServerError
	}

	http.Error(w, err.Error(), responseStatus)
}
