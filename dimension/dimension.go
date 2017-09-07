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

type Store struct {
	store.Storer
}

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
		InternalError(w, err)
		return
	}
	writeBody(w, bytes)
	log.Debug("get dimension nodes", log.Data{"instance": id})
}

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
		InternalError(w, err)
		return
	}
	writeBody(w, bytes)
	log.Debug("get dimension values", log.Data{"instance": id})

}

func (s *Store) AddNodeID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimensionName := vars["dimension"]
	value := vars["value"]
	nodeId := vars["node_id"]
	dim := models.Dimension{Name: dimensionName, Value: value, NodeId: nodeId, InstanceID: id}
	err := s.UpdateDimensionNodeID(&dim)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
}

func handleErrorType(err error, w http.ResponseWriter) {
	if strings.Contains(err.Error(), "not found") { // == api_errors.DatasetNotFound || err == api_errors.EditionNotFound || err == api_errors.VersionNotFound || err == api_errors.DimensionNodeNotFound || err == api_errors.InstanceNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func InternalError(w http.ResponseWriter, err error) {
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
