package dimension

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// Store provides a backend for dimensions
type Store struct {
	Auditor audit.AuditorService
	store.Storer
}

// List of audit actions for dimensions
const (
	GetDimensions                      = "getInstanceDimensions"
	GetUniqueDimensionAndOptionsAction = "getInstanceUniqueDimensionAndOptions"
	AddDimensionAction                 = "addDimension"
	UpdateNodeIDAction                 = "updateDimensionOptionWithNodeID"
)

func dimensionError(err error, message, action string) error {
	return errors.WithMessage(err, fmt.Sprintf("%v endpoint: %v", action, message))
}

// GetDimensionsHandler returns a list of all dimensions and their options for an instance resource
func (s *Store) GetDimensionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["id"]
	auditParams := common.Params{"instance_id": instanceID}
	logData := audit.ToLogData(auditParams)

	if auditErr := s.Auditor.Record(ctx, GetDimensions, audit.Attempted, auditParams); auditErr != nil {
		handleDimensionErr(ctx, w, auditErr, logData)
		return
	}

	b, err := s.getDimensions(ctx, instanceID, logData)
	if err != nil {
		if auditErr := s.Auditor.Record(ctx, GetDimensions, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleDimensionErr(ctx, w, err, logData)
		return
	}

	if auditErr := s.Auditor.Record(ctx, GetDimensions, audit.Successful, auditParams); auditErr != nil {
		handleDimensionErr(ctx, w, auditErr, logData)
		return
	}

	writeBody(ctx, w, b, GetDimensions, logData)
	log.InfoCtx(ctx, fmt.Sprintf("%v endpoint: successfully get dimensions for an instance resource", GetDimensions), logData)
}

func (s *Store) getDimensions(ctx context.Context, instanceID string, logData log.Data) ([]byte, error) {
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to get instance", GetDimensions), logData)
		return nil, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, dimensionError(err, "current instance has an invalid state", GetDimensions), logData)
		return nil, err
	}

	results, err := s.GetDimensionsFromInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to get dimension options for instance", GetDimensions), logData)
		return nil, err
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to marshal dimension nodes to json", GetDimensions), logData)
		return nil, err
	}

	return b, nil
}

// GetUniqueDimensionAndOptionsHandler returns a list of dimension options for a dimension of an instance
func (s *Store) GetUniqueDimensionAndOptionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["id"]
	dimension := vars["dimension"]
	auditParams := common.Params{"instance_id": instanceID, "dimension": dimension}
	logData := audit.ToLogData(auditParams)

	if auditErr := s.Auditor.Record(ctx, GetUniqueDimensionAndOptionsAction, audit.Attempted, auditParams); auditErr != nil {
		handleDimensionErr(ctx, w, auditErr, logData)
		return
	}

	b, err := s.getUniqueDimensionAndOptions(ctx, instanceID, dimension, logData)
	if err != nil {
		if auditErr := s.Auditor.Record(ctx, GetUniqueDimensionAndOptionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleDimensionErr(ctx, w, err, logData)
		return
	}

	if auditErr := s.Auditor.Record(ctx, GetUniqueDimensionAndOptionsAction, audit.Successful, auditParams); auditErr != nil {
		handleDimensionErr(ctx, w, auditErr, logData)
		return
	}

	writeBody(ctx, w, b, GetUniqueDimensionAndOptionsAction, logData)
	log.InfoCtx(ctx, fmt.Sprintf("%v endpoint: successfully get unique dimension options for an instance resource", GetUniqueDimensionAndOptionsAction), logData)
}

func (s *Store) getUniqueDimensionAndOptions(ctx context.Context, instanceID, dimension string, logData log.Data) ([]byte, error) {
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to get instance", GetUniqueDimensionAndOptionsAction), logData)
		return nil, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, dimensionError(err, "current instance has an invalid state", GetUniqueDimensionAndOptionsAction), logData)
		return nil, err
	}

	values, err := s.GetUniqueDimensionAndOptions(instanceID, dimension)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to get unique dimension values for instance", GetUniqueDimensionAndOptionsAction), logData)
		return nil, err
	}

	b, err := json.Marshal(values)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to marshal dimension values to json", GetUniqueDimensionAndOptionsAction), logData)
		return nil, err
	}

	return b, nil
}

// AddHandler represents adding a dimension to a specific instance
func (s *Store) AddHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	instanceID := vars["id"]
	auditParams := common.Params{"instance_id": instanceID}
	logData := audit.ToLogData(auditParams)

	option, err := unmarshalDimensionCache(r.Body)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to unmarshal dimension cache", AddDimensionAction), logData)

		if auditErr := s.Auditor.Record(ctx, AddDimensionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleDimensionErr(ctx, w, err, logData)
		return
	}

	if err := s.add(ctx, instanceID, option, logData); err != nil {
		if auditErr := s.Auditor.Record(ctx, AddDimensionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleDimensionErr(ctx, w, err, logData)
		return
	}

	s.Auditor.Record(ctx, AddDimensionAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "added dimension to instance resource", logData)
}

func (s *Store) add(ctx context.Context, instanceID string, option *models.CachedDimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to get instance", AddDimensionAction), logData)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, dimensionError(err, "current instance has an invalid state", AddDimensionAction), logData)
		return err
	}

	option.InstanceID = instanceID
	if err := s.AddDimensionToInstance(option); err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to upsert dimension for an instance", AddDimensionAction), logData)
		return err
	}

	return nil
}

// AddNodeIDHandler against a specific value for dimension
func (s *Store) AddNodeIDHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["id"]
	dimensionName := vars["dimension"]
	value := vars["value"]
	nodeID := vars["node_id"]
	auditParams := common.Params{"instance_id": instanceID, "dimension": dimensionName, "option": value, "node_id": nodeID}
	logData := audit.ToLogData(auditParams)

	dim := models.DimensionOption{Name: dimensionName, Option: value, NodeID: nodeID, InstanceID: instanceID}

	if err := s.addNodeID(ctx, dim, logData); err != nil {
		s.Auditor.Record(ctx, UpdateNodeIDAction, audit.Unsuccessful, auditParams)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	s.Auditor.Record(ctx, UpdateNodeIDAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "added node id to dimension of an instance resource", logData)
}

func (s *Store) addNodeID(ctx context.Context, dim models.DimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(dim.InstanceID)
	if err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to get instance", UpdateNodeIDAction), logData)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, dimensionError(err, "current instance has an invalid state", UpdateNodeIDAction), logData)
		return err
	}

	if err := s.UpdateDimensionNodeID(&dim); err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to update a dimension of that instance", UpdateNodeIDAction), logData)
		return err
	}

	return nil
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, action string, data log.Data) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		log.ErrorCtx(ctx, dimensionError(err, "failed to write response body", action), data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
