package instance

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

//Store provides a backend for instances
type Store struct {
	store.Storer
}

//GetList a list of all instances
func (s *Store) GetList(w http.ResponseWriter, r *http.Request) {
	stateFilter := r.URL.Query().Get("state")

	results, err := s.GetInstances(stateFilter)
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
	log.Debug("get all instances", log.Data{"query": stateFilter})
}

//Get a single instance by id
func (s *Store) Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	instance, err := s.GetInstance(id)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(instance)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, bytes)
	log.Debug("get all instances", nil)
}

//Add an instance
func (s *Store) Add(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	instance, err := unmarshalInstance(r.Body, true)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	instance, err = s.AddInstance(instance)
	if err != nil {
		internalError(w, err)
		return
	}

	bytes, err := json.Marshal(instance)
	if err != nil {
		internalError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	writeBody(w, bytes)
	log.Debug("add instance", log.Data{"instance": instance})
}

//Update a specific instance
func (s *Store) Update(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	defer r.Body.Close()

	instance, err := unmarshalInstance(r.Body, false)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = s.UpdateInstance(id, instance); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	log.Debug("updated instance", log.Data{"instance": id})
}

//UpdateObservations increments the count of inserted_observations against
//an instance
func (s *Store) UpdateObservations(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	insert := vars["inserted_observations"]

	observations, err := strconv.ParseInt(insert, 10, 64)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = s.UpdateObservationInserted(id, observations); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
	}
}

func unmarshalInstance(reader io.Reader, post bool) (*models.Instance, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var instance models.Instance
	err = json.Unmarshal(bytes, &instance)
	if err != nil {
		return nil, errors.New("Failed to parse json body: " + err.Error())
	}

	if post {
		if instance.Links.Job.ID == "" || instance.Links.Job.HRef == "" {
			return nil, errors.New("Missing job properties")
		}

		if instance.State == "" {
			instance.State = "created"
		}
	}
	return &instance, nil
}

func handleErrorType(err error, w http.ResponseWriter) {
	status := http.StatusInternalServerError

	if err == errs.DatasetNotFound || err == errs.EditionNotFound || err == errs.VersionNotFound || err == errs.DimensionNodeNotFound || err == errs.InstanceNotFound {
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
