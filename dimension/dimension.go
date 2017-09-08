package dimension

import (
	"encoding/json"
	"net/http"
	"strings"

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

//AddNodeID against a specific value for dimension
func (s *Store) AddNodeID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimensionName := vars["dimension"]
	value := vars["value"]
	nodeID := vars["node_id"]

	dim := models.Dimension{Name: dimensionName, Value: value, NodeID: nodeID, InstanceID: id}
	if err := s.UpdateDimensionNodeID(&dim); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
	}
}

func handleErrorType(err error, w http.ResponseWriter) {
	if strings.Contains(err.Error(), "not found") { // == api_errors.DatasetNotFound || err == api_errors.EditionNotFound || err == api_errors.VersionNotFound || err == api_errors.DimensionNodeNotFound || err == api_errors.InstanceNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
