package dimension

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	dphttp "github.com/ONSdigital/dp-net/http"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

// Store provides a backend for dimensions
type Store struct {
	store.Storer
}

// List of actions for dimensions
const (
	GetDimensions                      = "getInstanceDimensions"
	GetUniqueDimensionAndOptionsAction = "getInstanceUniqueDimensionAndOptions"
	AddDimensionAction                 = "addDimension"
	UpdateNodeIDAction                 = "updateDimensionOptionWithNodeID"
)

// GetDimensionsHandler returns a list of all dimensions and their options for an instance resource
func (s *Store) GetDimensionsHandler(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	logData := log.Data{"instance_id": instanceID}
	logData["action"] = GetDimensions

	// Get instance from MongoDB
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.Event(ctx, "failed to get instance", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Event(ctx, "current instance has an invalid state", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Get dimensions corresponding to the instance in the right state
	dimensions, totalCount, err := s.GetDimensionsFromInstance(ctx, instanceID, offset, limit)
	if err != nil {
		log.Event(ctx, "failed to get dimension options for instance", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	log.Event(ctx, "successfully get dimensions for an instance resource", log.INFO, logData)
	return dimensions, totalCount, nil
}

// GetUniqueDimensionAndOptionsHandler returns a list of dimension options for a dimension of an instance
func (s *Store) GetUniqueDimensionAndOptionsHandler(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimension := vars["dimension"]
	logData := log.Data{"instance_id": instanceID, "dimension": dimension}
	logData["action"] = GetUniqueDimensionAndOptionsAction

	// Get instance from MongoDB
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.Event(ctx, "failed to get instance", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Event(ctx, "current instance has an invalid state", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Get dimension options corresponding to the instance in the right state
	options, totalCount, err := s.GetUniqueDimensionAndOptions(ctx, instanceID, dimension, offset, limit)
	if err != nil {
		log.Event(ctx, "failed to get unique dimension options for instance", log.ERROR, log.Error(err), logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	log.Event(ctx, "successfully get unique dimension options for an instance resource", log.INFO, logData)
	return options, totalCount, nil
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

// createPatches manages the creation of an array of patch structs from the provided reader, and validates them
func createPatches(reader io.Reader) ([]dprequest.Patch, error) {
	patches := []dprequest.Patch{}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return []dprequest.Patch{}, apierrors.ErrInvalidBody
	}

	err = json.Unmarshal(bytes, &patches)
	if err != nil {
		return []dprequest.Patch{}, apierrors.ErrInvalidBody
	}

	for _, patch := range patches {
		if err := patch.Validate(dprequest.OpAdd); err != nil {
			return []dprequest.Patch{}, err
		}
	}
	return patches, nil
}

// PatchOptionHandler updates a dimension option according to the provided patch array body
func (s *Store) PatchOptionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimensionName := vars["dimension"]
	option := vars["option"]
	logData := log.Data{"instance_id": instanceID, "dimension": dimensionName, "option": option}

	// unmarshal and validate the patch array
	patches, err := createPatches(r.Body)
	if err != nil {
		log.Event(ctx, "error obtaining patch from request body", log.ERROR, log.Error(err), logData)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logData["patch_list"] = patches

	// apply the patches to the dimension option
	successfulPatches, err := s.patchOption(ctx, instanceID, dimensionName, option, patches, logData)
	if err != nil {
		logData["successful_patches"] = successfulPatches
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	// Marshal provided model
	b, err := json.Marshal(successfulPatches)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	// set content type and write response body
	setJSONPatchContentType(w)
	writeBody(ctx, w, b, logData)
	log.Event(ctx, "successfully patched dimension option of an instance resource", log.INFO, logData)
}

func (s *Store) patchOption(ctx context.Context, instanceID, dimensionName, option string, patches []dprequest.Patch, logData log.Data) (successful []dprequest.Patch, err error) {
	// apply patch operations sequentially, stop processing if one patch fails, and return a list of successful patches operations
	for _, patch := range patches {
		dimOption := models.DimensionOption{Name: dimensionName, Option: option, InstanceID: instanceID}

		// populate the field from the patch path
		switch patch.Path {
		case "/node_id":
			val, ok := patch.Value.(string)
			if !ok {
				return successful, apierrors.ErrInvalidPatch{Msg: "wrong value type for /node_id, expected string"}
			}
			dimOption.NodeID = val
		case "/order":
			// json numeric values are always float64
			v, ok := patch.Value.(float64)
			if !ok {
				return successful, apierrors.ErrInvalidPatch{Msg: "wrong value type for /order, expected numeric value (float64)"}
			}
			val := int(v)
			dimOption.Order = &val
		default:
			return successful, apierrors.ErrInvalidPatch{Msg: fmt.Sprintf("wrong path: %s", patch.Path)}
		}

		// update values in database
		if err := s.updateOption(ctx, dimOption, logData); err != nil {
			return successful, err
		}
		successful = append(successful, patch)
	}
	return successful, nil
}

// AddNodeIDHandler against a specific option for dimension
// Deprecated: this method is superseded by PatchOptionHandler
func (s *Store) AddNodeIDHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimensionName := vars["dimension"]
	option := vars["option"]
	nodeID := vars["node_id"]
	logData := log.Data{"instance_id": instanceID, "dimension": dimensionName, "option": option, "node_id": nodeID, "action": UpdateNodeIDAction}

	dimOption := models.DimensionOption{Name: dimensionName, Option: option, NodeID: nodeID, InstanceID: instanceID}

	if err := s.updateOption(ctx, dimOption, logData); err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	logData["action"] = AddDimensionAction
	log.Event(ctx, "added node id to dimension of an instance resource", log.INFO, logData)
}

// updateOption checks that the instance is in a valid state
// and then updates nodeID and order (if provided) to the provided dimension option
func (s *Store) updateOption(ctx context.Context, dimOption models.DimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(dimOption.InstanceID)
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

	if err := s.UpdateDimensionNodeIDAndOrder(&dimOption); err != nil {
		log.Event(ctx, "failed to update a dimension of that instance", log.ERROR, log.Error(err), logData)
		return err
	}

	return nil
}

func setJSONPatchContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json-patch+json")
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, data log.Data) {
	if _, err := w.Write(b); err != nil {
		log.Event(ctx, "failed to write response body", log.ERROR, log.Error(err), data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
