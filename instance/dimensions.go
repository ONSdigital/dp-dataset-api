package instance

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
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
	logData := log.Data{"instance_id": instanceID, "dimension": dimension, "action": UpdateDimensionAction}

	log.Event(ctx, "update instance dimension: update instance dimension", log.INFO, logData)

	if err := func() error {
		instance, err := s.GetInstance(instanceID)
		if err != nil {
			log.Event(ctx, "update instance dimension: Failed to GET instance", log.ERROR, log.Error(err), logData)
			return err
		}

		// Early return if instance state is invalid
		if err = models.CheckState("instance", instance.State); err != nil {
			logData["state"] = instance.State
			log.Event(ctx, "update instance dimension: current instance has an invalid state", log.ERROR, log.Error(err), logData)
			return err
		}

		// Read and unmarshal request body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Event(ctx, "update instance dimension: error reading request.body", log.ERROR, log.Error(err), logData)
			return errs.ErrUnableToReadMessage
		}

		var dim *models.Dimension

		err = json.Unmarshal(b, &dim)
		if err != nil {
			log.Event(ctx, "update instance dimension: failing to model models.Codelist resource based on request", log.ERROR, log.Error(err), logData)
			return errs.ErrUnableToParseJSON
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
			log.Event(ctx, "update instance dimension: dimension not found", log.ERROR, log.Error(errs.ErrDimensionNotFound), logData)
			return errs.ErrDimensionNotFound
		}

		// Only update dimensions of an instance
		instanceUpdate := &models.Instance{
			Dimensions:      instance.Dimensions,
			UniqueTimestamp: instance.UniqueTimestamp,
		}

		// Update instance
		if err = s.UpdateInstance(ctx, instanceID, instanceUpdate); err != nil {
			log.Event(ctx, "update instance dimension: failed to update instance with new dimension label/description", log.ERROR, log.Error(err), logData)
			return err
		}

		return nil
	}(); err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	log.Event(ctx, "updated instance dimension: request successful", log.INFO, logData)
}
