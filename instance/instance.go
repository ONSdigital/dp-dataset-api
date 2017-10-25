package instance

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

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
	stateFilterQuery := r.URL.Query().Get("state")
	var stateFilterList []string
	if stateFilterQuery != "" {
		stateFilterList = strings.Split(stateFilterQuery, ",")
		if err := models.ValidateStateFilter(stateFilterList); err != nil {
			log.Error(err, nil)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	results, err := s.GetInstances(stateFilterList)
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
	log.Debug("get all instances", log.Data{"query": stateFilterQuery})
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

	w.Header().Set("Content-Type", "application/json")

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

		// TODO Update dataset.next state to associated and add collection id
	case publishedState:
		if err = validateInstanceUpdate(associatedState, currentInstance, instance); err != nil {
			log.Error(err, log.Data{"instance_id": id, "current_state": currentInstance.State})
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		// TODO Update both edition and dataset states to published
	}

	if instance.State == editionConfirmedState {
		var editionDoc *models.Edition

		datasetID := currentInstance.Links.Dataset.ID
		instance.Links.Job = currentInstance.Links.Job

		// If instance has no edition, get the current edition
		if instance.Edition == "" {
			instance.Edition = currentInstance.Edition
		}

		edition := instance.Edition

		// Only create edition if it doesn't already exist
		editionDoc, err = s.GetEdition(datasetID, edition, "")
		if err != nil {
			if err != errs.EditionNotFound {
				log.Error(err, nil)
				handleErrorType(err, w)
			}

			// create unique id for edition
			editionID := uuid.NewV4().String()

			editionDoc = &models.Edition{
				ID:      editionID,
				Edition: edition,
				Links: &models.EditionLinks{
					Dataset: &models.LinkObject{
						ID:   datasetID,
						HRef: fmt.Sprintf("%s/datasets/%s", s.Host, datasetID),
					},
					Self: &models.LinkObject{
						HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", s.Host, datasetID, edition),
					},
					Versions: &models.LinkObject{
						HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions", s.Host, datasetID, edition),
					},
				},
				State: "created",
			}

			if err := s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
				log.ErrorR(r, err, nil)
				handleErrorType(err, w)
				return
			}

			log.Debug("created edition", log.Data{"instance": id, "edition": edition})
		}

		// Check whether instance has a version
		if currentInstance.Version < 1 {
			// Find the latest version for the dataset edition attached to this
			// instance and append by 1 to set the version of this instance document
			version, err := s.GetNextVersion(datasetID, edition)
			if err != nil {
				log.ErrorR(r, err, nil)
				handleErrorType(err, w)
				return
			}

			instance.Version = version

			links := s.defineInstanceLinks(instance, editionDoc)
			instance.Links = links
		}
	}

	if err = s.UpdateInstance(id, instance); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	log.Debug("updated instance", log.Data{"instance": id})
}

func validateInstanceUpdate(expectedState string, currentInstance, instance *models.Instance) error {
	if currentInstance.State != expectedState {
		err := fmt.Errorf("Unable to update resource, expected resource to have a state of %s", expectedState)
		return err
	}
	if instance.State == editionConfirmedState && currentInstance.Edition == "" && instance.Edition == "" {
		err := errors.New("Unable to update resource, missing a value for the edition")
		return err
	}

	return nil
}

func (s *Store) defineInstanceLinks(instance *models.Instance, editionDoc *models.Edition) models.InstanceLinks {
	stringifiedVersion := strconv.Itoa(instance.Version)

	links := models.InstanceLinks{
		Dataset: &models.IDLink{
			HRef: editionDoc.Links.Dataset.HRef,
			ID:   editionDoc.Links.Dataset.ID,
		},
		Dimensions: &models.IDLink{
			HRef: fmt.Sprintf("%s/versions/%s/dimensions", editionDoc.Links.Self.HRef, stringifiedVersion),
		},
		Edition: &models.IDLink{
			HRef: editionDoc.Links.Self.HRef,
			ID:   editionDoc.Edition,
		},
		Job: instance.Links.Job,
		Self: &models.IDLink{
			HRef: fmt.Sprintf("%s/versions/%s", editionDoc.Links.Self.HRef, stringifiedVersion),
		},
		Version: &models.IDLink{
			HRef: fmt.Sprintf("%s/versions/%s", editionDoc.Links.Self.HRef, stringifiedVersion),
			ID:   stringifiedVersion,
		},
	}

	return links
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
		if instance.Links.Job != nil {
			if instance.Links.Job.ID == "" || instance.Links.Job.HRef == "" {
				return nil, errors.New("Missing job properties")
			}
		} else {
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
