package instance

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/request"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// UpdateDimension updates label and/or description
// for a specific dimension within an instance
func (s *Store) UpdateDimension(w http.ResponseWriter, r *http.Request) {

	defer request.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimension := vars["dimension"]
	auditParams := common.Params{"instance_id": instanceID, "dimension": dimension}
	logData := audit.ToLogData(auditParams)

	log.InfoCtx(ctx, "update instance dimension", logData)

	if err := func() error {
		instance, err := s.GetInstance(instanceID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: Failed to GET instance"), logData)
			return err
		}
		auditParams["instance_state"] = instance.State

		// Early return if instance state is invalid
		if err = models.CheckState("instance", instance.State); err != nil {
			logData["state"] = instance.State
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: current instance has an invalid state"), logData)
			return err
		}

		// Read and unmarshal request body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: error reading request.body"), logData)
			return errs.ErrUnableToReadMessage
		}

		var dim *models.Dimension

		err = json.Unmarshal(b, &dim)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: failing to model models.Codelist resource based on request"), logData)
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
			log.ErrorCtx(ctx, errors.WithMessage(errs.ErrDimensionNotFound, "update instance dimension: dimension not found"), logData)
			return errs.ErrDimensionNotFound
		}

		// Only update dimensions of an instance
		instanceUpdate := &models.Instance{
			Dimensions:      instance.Dimensions,
			UniqueTimestamp: instance.UniqueTimestamp,
		}

		// Update instance
		if err = s.UpdateInstance(ctx, instanceID, instanceUpdate); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: failed to update instance with new dimension label/description"), logData)
			return err
		}

		return nil
	}(); err != nil {
		if auditErr := s.Auditor.Record(ctx, UpdateDimensionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	s.Auditor.Record(ctx, UpdateDimensionAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "updated instance dimension: request successful", logData)
}
