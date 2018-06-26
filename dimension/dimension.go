package dimension

import (
	"context"
	"encoding/json"
	"net/http"

	"io"
	"io/ioutil"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
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
	PostDimensionsAction = "postDimensions"
	PutNodeIDAction      = "putNodeID"
)

// GetDimensionsAndOptionsHandler returns a list of all dimensions and their options for an instance resource
func (s *Store) GetDimensionsAndOptionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["id"]
	logData := log.Data{"instance_id": instanceID}

	// Get instance
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "getNodes endpoint: failed to get instance"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, errors.Wrap(err, "current instance has an invalid state"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	results, err := s.GetDimensionsAndOptionsFromInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to get dimension nodes from instance"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to marshal dimension nodes to json"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	writeBody(ctx, w, b, logData)
	log.InfoCtx(ctx, "get dimension nodes", logData)
}

// GetUniqueDimensionAndOptionsHandler returns a list of dimension options for a dimension of an instance
func (s *Store) GetUniqueDimensionAndOptionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["id"]
	dimension := vars["dimension"]
	logData := log.Data{"instance_id": instanceID, "dimension": dimension}

	// Get instance
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "getDimensionValues endpoint: failed to get instance"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, errors.Wrap(err, "current instance has an invalid state"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	values, err := s.GetUniqueDimensionAndOptions(instanceID, dimension)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to get unique dimension values for instance"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	b, err := json.Marshal(values)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to marshal dimension values to json"), logData)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	writeBody(ctx, w, b, logData)
	log.InfoCtx(ctx, "get dimension values", logData)
}

// AddHandler represents adding a dimension to a specific instance
func (s *Store) AddHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	instanceID := vars["id"]
	logData := log.Data{"instance_id": instanceID}
	auditParams := common.Params{"instance_id": instanceID}

	option, err := unmarshalDimensionCache(r.Body)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "failed to unmarshal dimension cache"), logData)

		if auditErr := s.Auditor.Record(ctx, PostDimensionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleDimensionErr(ctx, w, err, logData)
		return
	}

	if err := s.add(ctx, instanceID, option, logData); err != nil {
		if auditErr := s.Auditor.Record(ctx, PostDimensionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleDimensionErr(ctx, w, err, logData)
		return
	}

	s.Auditor.Record(ctx, PostDimensionsAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "added dimension to instance resource", logData)
}

func (s *Store) add(ctx context.Context, instanceID string, option *models.CachedDimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "addDimensions endpoint: failed to get instance"), logData)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, errors.WithMessage(err, "current instance has an invalid state"), logData)
		return err
	}

	option.InstanceID = instanceID
	if err := s.AddDimensionToInstance(option); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "failed to upsert dimension for an instance"), logData)
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
	logData := log.Data{"instance_id": instanceID, "dimension_name": dimensionName, "option": value, "node_id": nodeID}
	auditParams := common.Params{"instance_id": instanceID, "dimension_name": dimensionName, "option": value, "node_id": nodeID}

	dim := models.DimensionOption{Name: dimensionName, Option: value, NodeID: nodeID, InstanceID: instanceID}

	if err := s.addNodeID(ctx, dim, logData); err != nil {
		s.Auditor.Record(ctx, PutNodeIDAction, audit.Unsuccessful, auditParams)
		handleDimensionErr(ctx, w, err, logData)
		return
	}

	s.Auditor.Record(ctx, PutNodeIDAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "added node id to dimension of an instance resource", logData)
}

func (s *Store) addNodeID(ctx context.Context, dim models.DimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(dim.InstanceID)
	if err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to get instance"), logData)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, errors.Wrap(err, "current instance has an invalid state"), logData)
		return err
	}

	if err := s.UpdateDimensionNodeID(&dim); err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to update a dimension of that instance"), logData)
		return err
	}

	return nil
}

func unmarshalDimensionCache(reader io.Reader) (*models.CachedDimensionOption, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var option models.CachedDimensionOption

	err = json.Unmarshal(b, &option)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON

	}
	if option.Name == "" || (option.Option == "" && option.CodeList == "") {
		return nil, errs.ErrMissingParameters
	}

	return &option, nil
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, data log.Data) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		log.ErrorCtx(ctx, errors.Wrap(err, "failed to write response body"), data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleDimensionErr(ctx context.Context, w http.ResponseWriter, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int
	resource := err
	switch {
	case errs.NotFoundMap[err]:
		status = http.StatusNotFound
	case errs.BadRequestMap[err]:
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
		resource = errs.ErrInternalServer
	}

	data["responseStatus"] = status
	audit.LogError(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, resource.Error(), status)
}
