package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	b, err := func() ([]byte, error) {
		datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getMetadata endpoint: get datastore.getDataset returned an error"), logData)
			return nil, err
		}

		authorised, logData := api.authenticate(r, logData)
		var state string

		// if request is authenticated then access resources of state other than published
		if !authorised {
			// Check for current sub document
			if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
				logData["dataset"] = datasetDoc.Current
				logError(ctx, errors.New("getMetadata endpoint: caller not is authorised and dataset but currently unpublished"), logData)
				return nil, errs.ErrDatasetNotFound
			}

			state = datasetDoc.Current.State
		}

		if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			logError(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to find edition for dataset"), logData)
			return nil, err
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to find version for dataset edition"), logData)
			return nil, errs.ErrMetadataVersionNotFound
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			logError(ctx, errors.WithMessage(err, "getMetadata endpoint: unpublished version has an invalid state"), logData)
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
			logError(ctx, errors.WithMessage(err, "getMetadata endpoint: failed to marshal metadata resource into bytes"), logData)
			return nil, err
		}
		return b, err
	}()

	if err != nil {
		handleMetadataErr(w, err)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get metadata relevant to version", logData)
}

func handleMetadataErr(w http.ResponseWriter, err error) {
	var responseStatus int

	switch {
	case err == errs.ErrEditionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrMetadataNoCurrentPublished:
		responseStatus = http.StatusNotFound
	case err == errs.ErrMetadataVersionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrDatasetNotFound:
		responseStatus = http.StatusNotFound
	default:
		responseStatus = http.StatusInternalServerError
	}

	http.Error(w, err.Error(), responseStatus)
}
