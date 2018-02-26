package dimension

import (
	"encoding/json"
	"net/http"

	"errors"
	"io"
	"io/ioutil"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

//Store provides a backend for dimensions
type Store struct {
	store.Storer
}

//GetNodes list from a specified instance
func (s *Store) GetNodes(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorC("Failed to GET instance", err, log.Data{"instance": id})
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

	bytes, err := json.Marshal(results)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, bytes)
	log.Debug("get dimension nodes", log.Data{"instance": id})
}

//GetUnique dimension values from a specified dimension
func (s *Store) GetUnique(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimension := vars["dimension"]

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorC("Failed to GET instance", err, log.Data{"instance": id})
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

	bytes, err := json.Marshal(values)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, bytes)
	log.Debug("get dimension values", log.Data{"instance": id})
}

// Add represents adding a dimension to a specific instance
func (s *Store) Add(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorC("Failed to GET instance", err, log.Data{"instance": id})
		handleErrorType(err, w)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		log.ErrorC("current instance has an invalid state", err, log.Data{"state": instance.State})
		handleErrorType(errs.ErrInternalServer, w)
		return
	}

	option, err := unmarshalDimensionCache(r.Body)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	option.InstanceID = id
	if err := s.AddDimensionToInstance(option); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
	}
}

//AddNodeID against a specific value for dimension
func (s *Store) AddNodeID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimensionName := vars["dimension"]
	value := vars["value"]
	nodeID := vars["node_id"]

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorC("Failed to GET instance when attempting to update a dimension of that instance.", err, log.Data{"instance": id})
		handleErrorType(err, w)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		log.ErrorC("current instance has an invalid state", err, log.Data{"state": instance.State})
		handleErrorType(errs.ErrInternalServer, w)
		return
	}

	dim := models.DimensionOption{Name: dimensionName, Option: value, NodeID: nodeID, InstanceID: id}
	if err := s.UpdateDimensionNodeID(&dim); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
	}
}

// CreateDataset manages the creation of a dataset from a reader
func unmarshalDimensionCache(reader io.Reader) (*models.CachedDimensionOption, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var option models.CachedDimensionOption

	err = json.Unmarshal(bytes, &option)
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

func writeBody(w http.ResponseWriter, bytes []byte) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(bytes); err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
