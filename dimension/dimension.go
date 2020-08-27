package dimension

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

// Store provides a backend for dimensions
type Store struct {
	store.Storer
}

// List of audit actions for dimensions
const (
	GetDimensions                      = "getInstanceDimensions"
	GetUniqueDimensionAndOptionsAction = "getInstanceUniqueDimensionAndOptions"
	AddDimensionAction                 = "addDimension"
	UpdateNodeIDAction                 = "updateDimensionOptionWithNodeID"
)

// GetDimensionsHandler returns a list of all dimensions and their options for an instance resource
func (s *Store) GetDimensionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	logData := log.Data{"instance_id": instanceID}
	logData["action"] = GetDimensions

	b, err := s.getDimensions(ctx, instanceID, logData)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}
	writeBody(ctx, w, b, logData)
	log.Event(ctx, "successfully get dimensions for an instance resource", log.INFO, logData)
}

func (s *Store) getDimensions(ctx context.Context, instanceID string, logData log.Data) ([]byte, error) {
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.Event(ctx, "failed to get instance", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Event(ctx, "current instance has an invalid state", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	results, err := s.GetDimensionsFromInstance(instanceID)
	if err != nil {
		log.Event(ctx, "failed to get dimension options for instance", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.Event(ctx, "failed to marshal dimension nodes to json", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	return b, nil
}

// GetUniqueDimensionAndOptionsHandler returns a list of dimension options for a dimension of an instance
func (s *Store) GetUniqueDimensionAndOptionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimension := vars["dimension"]
	logData := log.Data{"instance_id": instanceID, "dimension": dimension}
	logData["action"] = GetUniqueDimensionAndOptionsAction

	b, err := s.getUniqueDimensionAndOptions(ctx, instanceID, dimension, logData)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}
	writeBody(ctx, w, b, logData)
	log.Event(ctx, "successfully get unique dimension options for an instance resource", log.INFO, logData)
}

func (s *Store) getUniqueDimensionAndOptions(ctx context.Context, instanceID, dimension string, logData log.Data) ([]byte, error) {
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.Event(ctx, "failed to get instance", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Event(ctx, "current instance has an invalid state", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	options, err := s.GetUniqueDimensionAndOptions(instanceID, dimension)
	if err != nil {
		log.Event(ctx, "failed to get unique dimension options for instance", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	b, err := json.Marshal(options)
	if err != nil {
		log.Event(ctx, "failed to marshal dimension options to json", log.ERROR, log.Error(err), logData)
		return nil, err
	}

	return b, nil
}

// AddHandler represents adding a dimension to a specific instance
func (s *Store) AddHandler(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()

	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	logData := log.Data{"instance_id": instanceID}
	logData["action"] = AddDimensionAction

	option, err := unmarshalDimensionCache(r.Body)
	if err != nil {
		log.Event(ctx, "failed to unmarshal dimension cache", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	if err := s.add(ctx, instanceID, option, logData); err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}
	log.Event(ctx, "added dimension to instance resource", log.INFO, logData)
}

func (s *Store) add(ctx context.Context, instanceID string, option *models.CachedDimensionOption, logData log.Data) error {

	// Get instance
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.Event(ctx, "failed to get instance", log.ERROR, log.Error(err), logData)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Event(ctx, "current instance has an invalid state", log.ERROR, log.Error(err), logData)
		return err
	}

	option.InstanceID = instanceID
	if err := s.AddDimensionToInstance(option); err != nil {
		log.Event(ctx, "failed to upsert dimension for an instance", log.ERROR, log.Error(err), logData)
		return err
	}

	return nil
}

// AddNodeIDHandler against a specific option for dimension
func (s *Store) AddNodeIDHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimensionName := vars["dimension"]
	option := vars["option"]
	nodeID := vars["node_id"]
	logData := log.Data{"instance_id": instanceID, "dimension": dimensionName, "option": option, "node_id": nodeID, "action": UpdateNodeIDAction}

	dim := models.DimensionOption{Name: dimensionName, Option: option, NodeID: nodeID, InstanceID: instanceID}

	if err := s.addNodeID(ctx, dim, logData); err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	logData["action"] = AddDimensionAction
	log.Event(ctx, "added node id to dimension of an instance resource", log.INFO, logData)
}

func (s *Store) addNodeID(ctx context.Context, dim models.DimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(dim.InstanceID)
	if err != nil {
		log.Event(ctx, "failed to get instance", log.ERROR, log.Error(err), logData)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Event(ctx, "current instance has an invalid state", log.ERROR, log.Error(err), logData)
		return err
	}

	if err := s.UpdateDimensionNodeID(&dim); err != nil {
		log.Event(ctx, "failed to update a dimension of that instance", log.ERROR, log.Error(err), logData)
		return err
	}

	return nil
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, data log.Data) {

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		log.Event(ctx, "failed to write response body", log.ERROR, log.Error(err), data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
