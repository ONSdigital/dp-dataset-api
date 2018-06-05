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

// GetNodesHandler list from a specified instance
func (s *Store) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorC("failed to GET instance", err, log.Data{"instance": id})
		handleErrorType(err, w)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		log.ErrorC("current instance has an invalid state", err, log.Data{"state": instance.State})
		handleErrorType(errs.ErrInternalServer, w)
		return
	}

	results, err := s.GetDimensionNodesFromInstance(id)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, b)
	log.Debug("get dimension nodes", log.Data{"instance": id})
}

// GetUniqueHandler dimension values from a specified dimension
func (s *Store) GetUniqueHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimension := vars["dimension"]

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorC("failed to GET instance", err, log.Data{"instance": id})
		handleErrorType(err, w)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		log.ErrorC("current instance has an invalid state", err, log.Data{"state": instance.State})
		handleErrorType(errs.ErrInternalServer, w)
		return
	}

	values, err := s.GetUniqueDimensionValues(id, dimension)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	b, err := json.Marshal(values)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, b)
	log.Debug("get dimension values", log.Data{"instance": id})
}

// AddHandler represents adding a dimension to a specific instance
func (s *Store) AddHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	instanceID := vars["id"]
	logData := log.Data{"instance_id": instanceID}
	auditParams := common.Params{"instance_id": instanceID}

	statusCode, err := s.add(ctx, w, r, instanceID, logData)
	if err != nil {
		if auditErr := s.Auditor.Record(ctx, PostDimensionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			audit.LogActionFailure(ctx, PostDimensionsAction, audit.Unsuccessful, auditErr, logData)
		}
		handleDimensionErr(ctx, err, statusCode, w, logData)
		return
	}

	if auditErr := s.Auditor.Record(ctx, PostDimensionsAction, audit.Successful, auditParams); auditErr != nil {
		audit.LogActionFailure(ctx, PostDimensionsAction, audit.Successful, auditErr, logData)
	}

	audit.LogInfo(ctx, "added dimension to instance resource", logData)
}

func (s *Store) add(ctx context.Context, w http.ResponseWriter, r *http.Request, instanceID string, logData log.Data) (int, error) {
	// Get instance
	instance, err := s.GetInstance(instanceID)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "addDimensions endpoint: failed to GET instance"), logData)
		return 0, err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorCtx(ctx, errors.WithMessage(err, "current instance has an invalid state"), logData)
		return 0, err
	}

	option, err := unmarshalDimensionCache(r.Body)
	if err != nil {
		log.ErrorCtx(ctx, err, logData)
		return 400, err
	}

	option.InstanceID = instanceID
	if err := s.AddDimensionToInstance(option); err != nil {
		log.ErrorCtx(ctx, err, logData)
		return 0, err
	}

	return 201, nil
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

	if err := s.addNodeID(ctx, w, r, dim, logData); err != nil {
		if auditErr := s.Auditor.Record(ctx, PutNodeIDAction, audit.Unsuccessful, auditParams); auditErr != nil {
			audit.LogActionFailure(ctx, PutNodeIDAction, audit.Unsuccessful, auditErr, logData)
		}
		handleDimensionErr(ctx, err, 0, w, logData)
		return
	}

	if auditErr := s.Auditor.Record(ctx, PutNodeIDAction, audit.Successful, auditParams); auditErr != nil {
		audit.LogActionFailure(ctx, PutNodeIDAction, audit.Successful, auditErr, logData)
	}

	audit.LogInfo(ctx, "added node id to dimension of an instance resource", logData)
}

func (s *Store) addNodeID(ctx context.Context, w http.ResponseWriter, r *http.Request, dim models.DimensionOption, logData log.Data) error {
	// Get instance
	instance, err := s.GetInstance(dim.InstanceID)
	if err != nil {
		log.ErrorC("Failed to GET instance when attempting to update a dimension of that instance.", err, logData)
		handleErrorType(err, w)
		return err
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.ErrorC("current instance has an invalid state", err, logData)
		handleErrorType(errs.ErrInternalServer, w)
		return err
	}

	if err := s.UpdateDimensionNodeID(&dim); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return err
	}

	return nil
}

// CreateDataset manages the creation of a dataset from a reader
func unmarshalDimensionCache(reader io.Reader) (*models.CachedDimensionOption, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var option models.CachedDimensionOption

	err = json.Unmarshal(b, &option)
	if err != nil {
		return nil, errors.New("Failed to parse json body")

	}
	if option.Name == "" || (option.Option == "" && option.CodeList == "") {
		return nil, errors.New("Missing properties in JSON")
	}

	return &option, nil
}

func handleErrorType(err error, w http.ResponseWriter) {
	status := http.StatusInternalServerError

	if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound || err == errs.ErrVersionNotFound || err == errs.ErrDimensionNodeNotFound || err == errs.ErrInstanceNotFound {
		status = http.StatusNotFound
	}

	http.Error(w, err.Error(), status)

}

func internalError(w http.ResponseWriter, err error) {
	log.Error(err, nil)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeBody(w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleDimensionErr(ctx context.Context, err error, status int, w http.ResponseWriter, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	switch {
	case err == errs.ErrDatasetNotFound || err == errs.ErrInstanceNotFound || err == errs.ErrDimensionNodeNotFound:
		status = http.StatusNotFound
	default:
		if status == 0 {
			status = http.StatusInternalServerError
		}
	}

	data["responseStatus"] = status
	audit.LogError(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, err.Error(), status)
}
