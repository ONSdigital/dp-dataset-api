package v2

import (
	"context"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/api/common"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) PutDataset(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	eTag := common.GetIfMatch(r)
	logData := log.Data{"dataset_id": datasetID}

	newETag, err := func() (string, error) {

		updatedDataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.Error(ctx, "PutDataset endpoint: failed to model dataset resource based on request", err, logData)
			return "", errs.ErrAddUpdateDatasetBadRequest
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "PutDataset endpoint: datastore.getDataset returned an error", err, logData)
			return "", err
		}

		updatedDataset.Type = currentDataset.Next.Type

		models.CleanDataset(updatedDataset)

		err = api.validateRequest(ctx, currentDataset, updatedDataset, logData, eTag)
		if err != nil {
			log.Error(ctx, "PutDataset endpoint: failed to pass the request validation check", err, logData)
			return "", err
		}

		if updatedDataset.State == models.PublishedState {
			if err := common.PublishDataset(ctx, api.dataStore, currentDataset, nil); err != nil {
				log.Error(ctx, "PutDataset endpoint: failed to update dataset document to published", err, logData)
				return "", err
			}
		} else {
			newETag, err := api.dataStore.Backend.UpdateDataset(ctx, datasetID, eTag, updatedDataset)
			if err != nil {
				log.Error(ctx, "PutDataset endpoint: failed to update dataset resource", err, logData)
				return "", err
			}
			return newETag, nil
		}
		return "", nil
	}()

	if err != nil {
		common.HandleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	common.SetJSONContentType(w)
	if newETag != "" {
		common.SetETag(w, newETag)
	}
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "PutDataset endpoint: request successful", logData)
}

func (api *DatasetAPI) validateRequest(ctx context.Context, currentDataset *models.DatasetUpdate, updatedDataset *models.Dataset, logData log.Data, eTagSelector string) (err error) {

	if currentDataset.ETag != "" && currentDataset.ETag != eTagSelector {
		logData["incoming_ETag"] = eTagSelector
		logData["current_ETag"] = currentDataset.ETag
		log.Error(ctx, "ETag mismatch", errs.ErrDatasetConflict, logData)
		return errs.ErrDatasetConflict
	}

	if err = models.CheckState("dataset", updatedDataset.State); err != nil {
		logData["incoming_state"] = updatedDataset.State
		log.Error(ctx, "the incoming dataset object has an invalid state", err, logData)
		return err
	}

	if currentDataset.Next.State == models.PublishedState {
		logData["current_state"] = currentDataset.Next.State
		log.Error(ctx, "current dataset is published, therefore it can't be updated", errs.ErrResourcePublished, logData)
		return errs.ErrResourcePublished
	}

	if _, err = models.ValidateNomisURL(ctx, updatedDataset.Type, updatedDataset.NomisReferenceURL); err != nil {
		log.Error(ctx, "the incoming dataset contains a nomis URL but its type is not nomis", err, logData)
		return err
	}

	if err = models.ValidateDataset(updatedDataset); err != nil {
		log.Error(ctx, "failed field validation", err, logData)
		return err
	}

	return nil
}
