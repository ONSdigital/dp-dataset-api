package api

import (
	"encoding/json"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"net/http"
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
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bytes, err := json.Marshal(instance)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeBody(w, bytes)
	log.Debug("add instance", log.Data{"instance": instance})
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

func writeBody(w http.ResponseWriter, bytes []byte) {
	setJSONContentType(w)
	_, err := w.Write(bytes)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
