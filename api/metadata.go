package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	b, err := func() ([]byte, error) {
		versionId, err := strconv.Atoi(version)
		if err != nil {
			log.Event(ctx, "failed due to invalid version request", log.ERROR, log.Error(err), logData)
			return nil, errs.ErrInvalidVersion
		}
		if !(versionId > 0) {
			log.Event(ctx, "version is not a positive integer", log.ERROR, log.Error(err), logData)
			return nil, errs.ErrInvalidVersion

		}

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionId, "")
		if err != nil {
			if err == errs.ErrVersionNotFound {
				log.Event(ctx, "getMetadata endpoint: failed to find version for dataset edition", log.ERROR, log.Error(err), logData)
				return nil, errs.ErrMetadataVersionNotFound
			}
			log.Event(ctx, "getMetadata endpoint: get datastore.getVersion returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Event(ctx, "getMetadata endpoint: get datastore.getDataset returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		authorised := api.authenticate(r, logData)
		state := versionDoc.State

		// if the requested version is not yet published and the user is unauthorised, return a 404
		if !authorised && versionDoc.State != models.PublishedState {
			log.Event(ctx, "getMetadata endpoint: unauthorised user requested unpublished version, returning 404", log.ERROR, log.Error(errs.ErrUnauthorised), logData)
			return nil, errs.ErrUnauthorised
		}

		// if request is not authenticated but the version is published, restrict dataset access to only published resources
		if !authorised {
			// Check for current sub document
			if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
				logData["dataset"] = datasetDoc.Current
				log.Event(ctx, "getMetadata endpoint: caller not is authorised and dataset but currently unpublished", log.ERROR, log.Error(errors.New("document is not currently published")), logData)
				return nil, errs.ErrDatasetNotFound
			}

			state = datasetDoc.Current.State
		}

		if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
			log.Event(ctx, "getMetadata endpoint: failed to find edition for dataset", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			log.Event(ctx, "getMetadata endpoint: unpublished version has an invalid state", log.ERROR, log.Error(err), logData)
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
			log.Event(ctx, "getMetadata endpoint: failed to marshal metadata resource into bytes", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		return b, err
	}()

	if err != nil {
		log.Event(ctx, "received error", log.ERROR, log.Error(err), logData)
		handleMetadataErr(w, err)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Event(ctx, "getMetadata endpoint: failed to write bytes to response", log.ERROR, log.Error(err), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Event(ctx, "getMetadata endpoint: get metadata request successful", log.INFO, logData)
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
