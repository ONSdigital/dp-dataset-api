package api

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
)

func (api *DatasetAPI) getInstances(w http.ResponseWriter, r *http.Request) {
	results, err := api.dataStore.Backend.GetInstances()
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
	log.Debug("get all instances", nil)
}

func (api *DatasetAPI) getInstance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	instance, err := api.dataStore.Backend.GetInstance(id)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(instance)
	if err != nil {
		InternalError(w, err)
		return
	}
	writeBody(w, bytes)
	log.Debug("get all instances", nil)
}

func (api *DatasetAPI) addInstance(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	instance, err := models.CreateInstance(r.Body)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = instance.Defaults()
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	instance, err = api.dataStore.Backend.AddInstance(instance)
	if err != nil {
		InternalError(w, err)
		return
	}

	bytes, err := json.Marshal(instance)
	if err != nil {
		InternalError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeBody(w, bytes)
	log.Debug("add instance", log.Data{"instance": instance})
}

func (api *DatasetAPI) updateInstance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	defer r.Body.Close()
	instance, err := models.CreateInstance(r.Body)
	id := vars["id"]
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = api.dataStore.Backend.UpdateInstance(id, instance)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
	log.Debug("updated instance", log.Data{"instance": id})
}

func (api *DatasetAPI) addEventToInstance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	defer r.Body.Close()
	event, err := models.CreateEvent(r.Body)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = event.Validate()
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = api.dataStore.Backend.AddEventToInstance(id, event)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
	log.Debug("add event to instance", log.Data{"instance": id})

}

func (api *DatasetAPI) addDimensionToInstance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimensionName := vars["dimension"]
	value := vars["value"]
	dim := models.DimensionNode{Name: dimensionName, Value: value, InstanceID: id}
	err := api.dataStore.Backend.AddDimensionToInstance(&dim)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
}

func (api *DatasetAPI) addNodeIdToDimension(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimensionName := vars["dimension"]
	value := vars["value"]
	nodeId := vars["node_id"]
	dim := models.DimensionNode{Name: dimensionName, Value: value, NodeId: nodeId, InstanceID: id}
	err := api.dataStore.Backend.UpdateDimensionNodeID(&dim)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
}

func (api *DatasetAPI) updateObservations(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	observations, err := strconv.ParseInt(vars["inserted_observations"], 10, 64)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = api.dataStore.Backend.UpdateObservationInserted(id, observations)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
}

func (api *DatasetAPI) getDimensionNodes(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	results, err := api.dataStore.Backend.GetDimensionNodesFromInstance(id)
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

func (api *DatasetAPI) getUniqueDimensions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dimension := vars["dimension"]
	values, err := api.dataStore.Backend.GetUniqueDimensionValues(id, dimension)
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

func InternalError(w http.ResponseWriter, err error) {
	log.Error(err, nil)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeBody(w http.ResponseWriter, bytes []byte) {
	setJSONContentType(w)
	_, err := w.Write(bytes)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
