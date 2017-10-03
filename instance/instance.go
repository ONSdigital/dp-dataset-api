package instance

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	uuid "github.com/satori/go.uuid"
)

//Store provides a backend for instances
type Store struct {
	Host string
	store.Storer
}

const (
	completedState        = "completed"
	editionConfirmedState = "edition-confirmed"
	associatedState       = "associated"
	publishedState        = "published"
)

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

	// Get the current document to check the state of instance
	currentInstance, err := s.GetInstance(id)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	switch instance.State {
	case editionConfirmedState:
		if err = validateInstanceUpdate(completedState, currentInstance, instance); err != nil {
			log.Error(err, log.Data{"instance_id": id, "current_state": currentInstance.State})
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	case associatedState:
		if err = validateInstanceUpdate(editionConfirmedState, currentInstance, instance); err != nil {
			log.Error(err, log.Data{"instance_id": id, "current_state": currentInstance.State})
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	case publishedState:
		if err = validateInstanceUpdate(associatedState, currentInstance, instance); err != nil {
			log.Error(err, log.Data{"instance_id": id, "current_state": currentInstance.State})
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	if err = s.UpdateInstance(id, instance); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	if instance.State == editionConfirmedState {
		var editionDoc *models.Edition

		datasetID := currentInstance.Links.Dataset.ID
		edition := currentInstance.Edition
		if instance.Edition != "" {
			edition = instance.Edition
		}

		// Set the instance edition to the latest version
		instance.Edition = edition

		editionDoc = &models.Edition{
			Edition: edition,
			Links: models.EditionLinks{
				Dataset: models.LinkObject{
					ID:   datasetID,
					HRef: fmt.Sprintf("%s/datasets/%s", s.Host, datasetID),
				},
				Self: models.LinkObject{
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", s.Host, datasetID, edition),
				},
				Versions: models.LinkObject{
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions", s.Host, datasetID, edition),
				},
			},
		}

		if err := s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
			log.ErrorR(r, err, nil)
			handleErrorType(err, w)
			return
		}

		// Check all versions of edition to see if a version already exists
		// for this instance (use the instance_id)
		if _, err = s.GetVersionByInstanceID(id); err != nil {
			if err == errs.VersionNotFound {
				if err := s.createVersion(instance, editionDoc); err != nil {
					log.ErrorR(r, err, nil)
					handleErrorType(err, w)
				}
			} else {
				log.ErrorR(r, err, nil)
				handleErrorType(err, w)
			}
		} else {
			log.Debug("version already exists for instance", log.Data{"instance_id": id})
		}
	}

	log.Debug("updated instance", log.Data{"instance": id})
}

func validateInstanceUpdate(expectedState string, currentInstance, instance *models.Instance) error {
	if currentInstance.State != expectedState {
		err := errors.New("Unable to update resource, edition not confirmed")
		return err
	}
	if instance.Edition != currentInstance.Edition && instance.Edition != "" {
		err := errors.New("Unable to update resource, edition has already been confirmed")
		return err
	}

	return nil
}

func (s *Store) createVersion(instance *models.Instance, editionDoc *models.Edition) error {
	// Find the latest version to be able to increment the version
	// number before creating a new version document
	nextVersion, err := s.GetNextVersion(editionDoc.Links.Dataset.ID, instance.Edition)
	if err != nil {
		return err
	}

	versionID := (uuid.NewV4()).String()

	version := &models.Version{}

	version.ID = versionID
	version.Edition = instance.Edition
	version.InstanceID = instance.InstanceID
	version.Links.Dataset = editionDoc.Links.Dataset
	version.Links.Dimensions.HRef = fmt.Sprintf("%s/instance/%s/dimensions/", s.Host, instance.InstanceID)
	version.Links.Edition.ID = editionDoc.ID
	version.Links.Edition.HRef = editionDoc.Links.Self.HRef
	version.Links.Self.HRef = fmt.Sprintf("%s/versions/%s", editionDoc.Links.Self.HRef, versionID)
	version.State = "created"
	version.Version = nextVersion

	if err := s.UpsertVersion(versionID, version); err != nil {
		return err
	}

	return nil
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
