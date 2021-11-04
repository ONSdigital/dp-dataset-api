package instance

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

// UpdateDimension updates label and/or description
// for a specific dimension within an instance
func (s *Store) UpdateDimension(w http.ResponseWriter, r *http.Request) {

	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimension := vars["dimension"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID, "dimension": dimension}

	log.Info(ctx, "update instance dimension: update instance dimension", logData)

	// Acquire instance lock to make sure that this call does not interfere with any other 'write' call against the same instance
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
	}
	defer s.UnlockInstance(ctx, lockID)

	instance, err := s.GetInstance(ctx, instanceID, eTag)
	if err != nil {
		log.Error(ctx, "update instance dimension: Failed to GET instance", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Error(ctx, "update instance dimension: current instance has an invalid state", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	// Read and unmarshal request body
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(ctx, "update instance dimension: error reading request.body", err, logData)
		handleInstanceErr(ctx, errs.ErrUnableToReadMessage, w, logData)
		return
	}

	var dim *models.Dimension

	err = json.Unmarshal(b, &dim)
	if err != nil {
		log.Error(ctx, "update instance dimension: failing to model models.Codelist resource based on request", err, logData)
		handleInstanceErr(ctx, errs.ErrUnableToParseJSON, w, logData)
		return
	}

	// Update instance-dimension
	notFound := true
	for i := range instance.Dimensions {

		// For the chosen dimension
		if instance.Dimensions[i].Name == dimension {
			notFound = false
			// Assign update info, conditionals to allow updating
			// of both or either without blanking other
			if dim.Label != "" {
				instance.Dimensions[i].Label = dim.Label
			}
			if dim.Description != "" {
				instance.Dimensions[i].Description = dim.Description
			}
			break
		}
	}

	if notFound {
		log.Error(ctx, "update instance dimension: dimension not found", errs.ErrDimensionNotFound, logData)
		handleInstanceErr(ctx, errs.ErrDimensionNotFound, w, logData)
		return
	}

	// Only update dimensions of an instance
	instanceUpdate := &models.Instance{
		Dimensions:      instance.Dimensions,
		UniqueTimestamp: instance.UniqueTimestamp,
	}

	// Update instance
	newETag, err := s.UpdateInstance(ctx, instance, instanceUpdate, eTag)
	if err != nil {
		log.Error(ctx, "update instance dimension: failed to update instance with new dimension label/description", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	log.Info(ctx, "updated instance dimension: request successful", logData)

	setETag(w, newETag)
}
