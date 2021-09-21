package dimension

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/utils"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

// Store provides a backend for dimensions
type Store struct {
	store.Storer
	MaxRequestOptions int
}

// List of actions for dimensions
const (
	GetDimensions                      = "getInstanceDimensions"
	GetUniqueDimensionAndOptionsAction = "getInstanceUniqueDimensionAndOptions"
	AddDimensionAction                 = "addDimension"
	UpdateNodeIDAction                 = "updateDimensionOptionWithNodeID"
)

// Regex const for patch paths
var (
	optionNodeIDRegex = regexp.MustCompile("/([^/]+)/options/([^/]+)/node_id")
	optionOrderRegex  = regexp.MustCompile("/([^/]+)/options/([^/]+)/order")
	slashRegex        = regexp.MustCompile("/")
)

// GetDimensionsHandler returns a list of all dimensions and their options for an instance resource
func (s *Store) GetDimensionsHandler(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID}
	logData["action"] = GetDimensions

	// acquire instance lock to make sure we read the correct values of dimension options
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		return nil, 0, err
	}
	defer s.UnlockInstance(lockID)

	// Get instance from MongoDB
	instance, err := s.GetInstance(instanceID, eTag)
	if err != nil {
		log.Error(ctx, "failed to get instance", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Error(ctx, "current instance has an invalid state", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Get dimensions corresponding to the instance in the right state
	dimensions, totalCount, err := s.GetDimensionsFromInstance(ctx, instanceID, offset, limit)
	if err != nil {
		log.Error(ctx, "failed to get dimension options for instance", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	log.Info(ctx, "successfully get dimensions for an instance resource", logData)
	setETag(w, instance.ETag)
	return dimensions, totalCount, nil
}

// GetUniqueDimensionAndOptionsHandler returns a list of dimension options for a dimension of an instance
func (s *Store) GetUniqueDimensionAndOptionsHandler(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimension := vars["dimension"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID, "dimension": dimension}
	logData["action"] = GetUniqueDimensionAndOptionsAction

	// acquire instance lock to make sure we read the correct values of dimension options
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		return nil, 0, err
	}
	defer s.UnlockInstance(lockID)

	// Get instance from MongoDB
	instance, err := s.GetInstance(instanceID, eTag)
	if err != nil {
		log.Error(ctx, "failed to get instance", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Error(ctx, "current instance has an invalid state", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// Get dimension options corresponding to the instance in the right state
	// Note: GetUniqueDimensionAndOptions does not implement pagination at query level
	options, totalCount, err := s.GetUniqueDimensionAndOptions(ctx, instanceID, dimension, offset, limit)
	if err != nil {
		log.Error(ctx, "failed to get unique dimension options for instance", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return nil, 0, err
	}

	// create the paginated result by cutting the slice
	slicedOptions := []*string{}
	if limit > 0 {
		slicedOptions = utils.SliceStr(options, offset, limit)
	}

	log.Info(ctx, "successfully get unique dimension options for an instance resource", logData)
	setETag(w, instance.ETag)
	return slicedOptions, totalCount, nil
}

// AddHandler represents adding a dimension to a specific instance
func (s *Store) AddHandler(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()

	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID}
	logData["action"] = AddDimensionAction

	option, err := unmarshalDimensionCache(r.Body)
	if err != nil {
		log.Error(ctx, "failed to unmarshal dimension cache", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		log.Error(ctx, "failed to acquire lock", err, logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}
	defer s.UnlockInstance(lockID)

	// upsert dimension option
	newETag, _, err := s.upsertAndUpdate(ctx, instanceID, []*models.CachedDimensionOption{option}, nil, logData, eTag)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}
	log.Info(ctx, "added dimension to instance resource", logData)

	setETag(w, newETag)
}

// PatchDimensionsHandler represents adding multile dimensions to a specific instance
func (s *Store) PatchDimensionsHandler(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID}

	// unmarshal and validate the patch array
	patches, err := createPatches(r.Body, dprequest.OpAdd)
	if err != nil {
		log.Error(ctx, "error obtaining patch from request body", err, logData)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logData["num_patches"] = len(patches)

	// apply the patches to the instance dimensions
	successfulPatches, newETag, err := s.applyPatchesForDimensions(ctx, instanceID, patches, logData, eTag)
	if err != nil {
		logData["successful_patches"] = successfulPatches
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	// Marshal successful patches response
	b, err := json.Marshal(successfulPatches)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	// set content type and write response body
	setJSONPatchContentType(w)
	setETag(w, newETag)
	writeBody(ctx, w, b, logData)
	log.Info(ctx, "successfully patched dimensions of an instance resource", logData)
}

func (s *Store) applyPatchesForDimensions(ctx context.Context, instanceID string, patches []dprequest.Patch, logData log.Data, eTagSelector string) (successful []dprequest.Patch, newETag string, err error) {
	upserts := []dprequest.Patch{} // list of patches that correspond to a MongoDB upsert
	updates := []dprequest.Patch{} // list of patches that correspond to a MongoDB update

	optionsToUpdate := []*models.DimensionOption{}       // values to update (extracted from '/{dimension}/options/{option}/node_id' and '/{dimension}/options/{option}/order')
	optionsToUpsert := []*models.CachedDimensionOption{} // full dimension options to upsert (extracted from '/-')

	// 1- Populate slices to update, failing if there is any wrong path or value (nothing is written to the DB yet)
	for _, patch := range patches {
		// check that we did not reach maximum size
		if len(optionsToUpdate)+len(optionsToUpsert) > s.MaxRequestOptions {
			logData["max_options"] = s.MaxRequestOptions
			return nil, "", apierrors.ErrInvalidPatch{Msg: fmt.Sprintf("a maximum of %d overall dimension values can be provied in a set of patch operations, which has been exceeded", s.MaxRequestOptions)}
		}

		if patch.Path == "/-" {
			// get list of options provided as value
			options, err := getOptionsArrayFromInterface(patch.Value)
			if err != nil {
				return nil, "", apierrors.ErrInvalidPatch{Msg: fmt.Sprintf("provided values '%#v' is not a list of dimension options", patch.Value)}
			}
			optionsToUpsert = append(optionsToUpsert, options...)
			upserts = append(upserts, patch)
			continue
		}

		// check if path is '/{dimension}/options/{option}/node_id':
		if len(optionNodeIDRegex.FindAllString(patch.Path, -1)) == 1 {
			val, ok := patch.Value.(string)
			if !ok {
				return nil, "", apierrors.ErrInvalidPatch{Msg: "wrong value type for /{dimension}/options/{option}/node_id, expected string"}
			}
			spl := slashRegex.Split(patch.Path, 5)
			optionsToUpdate = append(optionsToUpdate, &models.DimensionOption{
				Name:       spl[1],
				Option:     spl[3],
				InstanceID: instanceID,
				NodeID:     val,
			})
			updates = append(updates, patch)
			continue
		}

		// check if path is '/{dimension}/options/{option}/order':
		if len(optionOrderRegex.FindAllString(patch.Path, -1)) == 1 {
			v, ok := patch.Value.(float64)
			if !ok {
				return successful, "", apierrors.ErrInvalidPatch{Msg: "wrong value type for /order, expected numeric value (float64)"}
			}
			val := int(v)
			spl := slashRegex.Split(patch.Path, 5)
			optionsToUpdate = append(optionsToUpdate, &models.DimensionOption{
				Name:       spl[1],
				Option:     spl[3],
				InstanceID: instanceID,
				Order:      &val,
			})
			updates = append(updates, patch)
			continue
		}

		// any other path is not supported
		return nil, "", apierrors.ErrInvalidPatch{Msg: fmt.Sprintf("provided path '%s' not supported. Supported paths: '/-', '/{dimension}/options/{option}/node_id', '/{dimension}/options/{option}/order'", patch.Path)}
	}

	// check that we did not reach maximum size
	if len(optionsToUpdate)+len(optionsToUpsert) > s.MaxRequestOptions {
		logData["max_options"] = s.MaxRequestOptions
		return nil, "", apierrors.ErrInvalidPatch{Msg: fmt.Sprintf("a maximum of %d overall dimension values can be provied in a set of patch operations, which has been exceeded", s.MaxRequestOptions)}
	}

	// acquire instance lock so that the instance update and the dimension.options update are atomic
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		return nil, "", err
	}
	defer s.UnlockInstance(lockID)

	// Upsert and update dimension options
	upsertOK := false
	newETag, upsertOK, err = s.upsertAndUpdate(ctx, instanceID, optionsToUpsert, optionsToUpdate, logData, eTagSelector)
	if err != nil {
		if upsertOK {
			successful = append(successful, upserts...)
		}
		return successful, "", err
	}

	successful = append(successful, updates...)
	return successful, newETag, nil
}

// upsertAndUpdate checks that the instance is in a valid state,
// then it updates its ETag value according to the upserts and updates
// and then performs the upserts (inert or update) and updates (node id and order) in bulk
func (s *Store) upsertAndUpdate(ctx context.Context, instanceID string, optionsToUpsert []*models.CachedDimensionOption, optionsToUpdate []*models.DimensionOption, logData log.Data, eTagSelector string) (newETag string, upsertOK bool, err error) {
	if len(optionsToUpsert) == 0 && len(optionsToUpdate) == 0 {
		return "", true, nil // nothing to update or upsert
	}

	// Get instance
	instance, err := s.GetInstance(instanceID, eTagSelector)
	if err != nil {
		log.Error(ctx, "failed to get instance", err, logData)
		return "", false, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Error(ctx, "current instance has an invalid state", err, logData)
		return "", false, err
	}

	// set instanceID in all options
	for _, option := range optionsToUpsert {
		option.InstanceID = instanceID
	}
	for _, option := range optionsToUpdate {
		option.InstanceID = instanceID
	}

	// generate a new unique ETag for the instance + options and update it in DB
	newETag, err = s.UpdateETagForOptions(instance, optionsToUpsert, optionsToUpdate, eTagSelector)
	if err != nil {
		log.Error(ctx, "failed to update eTag for an instance", err, logData)
		return "", false, err
	}

	// Upsert dimension options in bulk
	if len(optionsToUpsert) > 0 {
		if err := s.UpsertDimensionsToInstance(optionsToUpsert); err != nil {
			log.Error(ctx, "failed to upsert dimensions for an instance", err, logData)
			return "", false, err
		}
	}

	// Update dimension options NodeID and Order values
	if len(optionsToUpdate) > 0 {
		if err := s.UpdateDimensionsNodeIDAndOrder(optionsToUpdate); err != nil {
			log.Error(ctx, "failed to update a dimension of that instance", err, logData)
			return "", true, err
		}
	}

	return newETag, true, nil
}

// createPatches manages the creation of an array of patch structs from the provided reader, and validates them
func createPatches(reader io.Reader, supportedOps ...dprequest.PatchOp) ([]dprequest.Patch, error) {
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
		if err := patch.Validate(supportedOps...); err != nil {
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
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID, "dimension": dimensionName, "option": option}

	// unmarshal and validate the patch array
	patches, err := createPatches(r.Body, dprequest.OpAdd) // OpAdd Upserts all the items provided in the value array
	if err != nil {
		log.Error(ctx, "error obtaining patch from request body", err, logData)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logData["patch_list"] = patches

	// apply the patches to the dimension option
	successfulPatches, newETag, err := s.patchOption(ctx, instanceID, dimensionName, option, patches, logData, eTag)
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
	setETag(w, newETag)
	writeBody(ctx, w, b, logData)
	log.Info(ctx, "successfully patched dimension option of an instance resource", logData)
}

func (s *Store) patchOption(ctx context.Context, instanceID, dimensionName, option string, patches []dprequest.Patch, logData log.Data, eTagSelector string) (successful []dprequest.Patch, newETag string, err error) {
	// acquire instance lock so that the instance update and the dimension.options update are atomic
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		return successful, "", err
	}
	defer s.UnlockInstance(lockID)

	// apply patch operations sequentially, stop processing if one patch fails, and return a list of successful patches operations
	for _, patch := range patches {
		dimOption := models.DimensionOption{Name: dimensionName, Option: option, InstanceID: instanceID}

		// populate the field from the patch path
		switch patch.Path {
		case "/node_id":
			val, ok := patch.Value.(string)
			if !ok {
				return successful, "", apierrors.ErrInvalidPatch{Msg: "wrong value type for /node_id, expected string"}
			}
			dimOption.NodeID = val
		case "/order":
			// json numeric values are always float64
			v, ok := patch.Value.(float64)
			if !ok {
				return successful, "", apierrors.ErrInvalidPatch{Msg: "wrong value type for /order, expected numeric value (float64)"}
			}
			val := int(v)
			dimOption.Order = &val
		default:
			return successful, "", apierrors.ErrInvalidPatch{Msg: fmt.Sprintf("wrong path: %s", patch.Path)}
		}

		// update values in database, updating the instance eTag
		newETag, _, err = s.upsertAndUpdate(ctx, dimOption.InstanceID, nil, []*models.DimensionOption{&dimOption}, logData, eTagSelector)
		if err != nil {
			return successful, "", err
		}
		successful = append(successful, patch)
		eTagSelector = newETag
	}
	return successful, newETag, nil
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
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID, "dimension": dimensionName, "option": option, "node_id": nodeID, "action": UpdateNodeIDAction}

	dimOption := models.DimensionOption{Name: dimensionName, Option: option, NodeID: nodeID, InstanceID: instanceID}

	// acquire instance lock so that the instance update and the dimension.options update are atomic
	lockID, err := s.AcquireInstanceLock(ctx, dimOption.InstanceID)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}
	defer s.UnlockInstance(lockID)

	newETag, _, err := s.upsertAndUpdate(ctx, dimOption.InstanceID, nil, []*models.DimensionOption{&dimOption}, logData, eTag)
	if err != nil {
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	logData["action"] = AddDimensionAction
	log.Info(ctx, "added node id to dimension of an instance resource", logData)
	setETag(w, newETag)
}

func setJSONPatchContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json-patch+json")
}

func getIfMatch(r *http.Request) string {
	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		return mongo.AnyETag
	}
	return ifMatch
}

func setETag(w http.ResponseWriter, eTag string) {
	w.Header().Set("ETag", eTag)
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, data log.Data) {
	if _, err := w.Write(b); err != nil {
		log.Error(ctx, "failed to write response body", err, data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// getOptionsArrayFromInterface obtains an array of *CachedDimensionOption from the provided interface
func getOptionsArrayFromInterface(elements interface{}) ([]*models.CachedDimensionOption, error) {
	options := []*models.CachedDimensionOption{}

	// elements should be an array
	arr, ok := elements.([]interface{})
	if !ok {
		return options, errors.New("missing list of items")
	}

	// each item in the array should be an option
	for _, v := range arr {
		// need to re-marshal, as it is currently a map
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		// unmarshal and validate CachedDimensionOption structure
		option, err := unmarshalDimensionCache(bytes.NewBuffer(b))
		if err != nil {
			return nil, err
		}
		options = append(options, option)
	}

	return options, nil
}
