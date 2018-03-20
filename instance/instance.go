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
	"github.com/satori/go.uuid"
)

//Store provides a backend for instances
type Store struct {
	Host string
	store.Storer
}

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

	b, err := json.Marshal(results)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, b)
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

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		log.ErrorC("instance has an invalid state", err, log.Data{"state": instance.State})
		internalError(w, err)
		return
	}

	b, err := json.Marshal(instance)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, b)
	log.Debug("get instance", log.Data{"instance_id": id})
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

	instance.InstanceID = uuid.NewV4().String()
	instance.Links.Self = &models.IDLink{
		HRef: fmt.Sprintf("%s/instances/%s", s.Host, instance.InstanceID),
	}

	instance, err = s.AddInstance(instance)
	if err != nil {
		internalError(w, err)
		return
	}

	b, err := json.Marshal(instance)
	if err != nil {
		internalError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusCreated)
	writeBody(w, b)
	log.Debug("add instance", log.Data{"instance": instance})
}

// UpdateDimension updates label and/or description for a specific dimension within an instance
func (s *Store) UpdateDimension(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	id := vars["id"]
	dimension := vars["dimension"]

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

	// Early return if instance is already published
	if instance.State == models.PublishedState {
		log.Debug("unable to update instance/version, already published", nil)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Read and unmarshal request body
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.ErrorC("Error reading response.body.", err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var dim *models.CodeList

	err = json.Unmarshal(b, &dim)
	if err != nil {
		log.ErrorC("Failing to model models.Codelist resource based on request", err, log.Data{"instance": id, "dimension": dimension})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Update instance-dimension
	notFound := true
	for i := range instance.Dimensions {

		// For the chosen dimension
		if instance.Dimensions[i].Name == dimension {
			notFound = false
			// Assign update info, conditionals to allow updating of both or either without blanking other
			if dim.Label != "" {
				instance.Dimensions[i].Label = dim.Label
			}
			if dim.Description != "" {
				instance.Dimensions[i].Description = dim.Description
			}
			break
		}

	}

	if notFound {
		log.ErrorC("dimension not found", errs.ErrDimensionNotFound, log.Data{"instance": id, "dimension": dimension})
		handleErrorType(errs.ErrDimensionNotFound, w)
		return
	}

	// Update instance
	if err = s.UpdateInstance(id, instance); err != nil {
		log.ErrorC("Failed to update instance with new dimension label/description.", err, log.Data{"instance": id, "dimension": dimension})
		handleErrorType(err, w)
		return
	}

	log.Debug("updated dimension", log.Data{"instance": id, "dimension": dimension})

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

	// Get the current document
	currentInstance, err := s.GetInstance(id)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", currentInstance.State); err != nil {
		log.ErrorC("current instance has an invalid state", err, log.Data{"state": currentInstance.State})
		handleErrorType(errs.ErrInternalServer, w)
		return
	}

	// Combine existing links and spatial link
	instance.Links = updateLinks(instance, currentInstance)

	logData := log.Data{"instance_id": id, "current_state": currentInstance.State, "requested_state": instance.State}
	if instance.State != currentInstance.State {
		switch instance.State {
		case models.CompletedState:
			if err = validateInstanceUpdate(models.SubmittedState, currentInstance, instance); err != nil {
				log.Error(err, logData)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		case models.EditionConfirmedState:
			if err = validateInstanceUpdate(models.CompletedState, currentInstance, instance); err != nil {
				log.Error(err, logData)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		case models.AssociatedState:
			if err = validateInstanceUpdate(models.EditionConfirmedState, currentInstance, instance); err != nil {
				log.Error(err, logData)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}

			// TODO Update dataset.next state to associated and add collection id
		case models.PublishedState:
			if err = validateInstanceUpdate(models.AssociatedState, currentInstance, instance); err != nil {
				log.Error(err, logData)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}

			// TODO Update both edition and dataset states to published
		}
	}

	if instance.State == models.EditionConfirmedState {
		datasetID := currentInstance.Links.Dataset.ID

		// If instance has no edition, get the current edition
		if instance.Edition == "" {
			instance.Edition = currentInstance.Edition
		}
		edition := instance.Edition

		// Only create edition if it doesn't already exist
		editionDoc, err := s.getEdition(datasetID, edition, id)
		if err != nil {
			handleErrorType(err, w)
			return
		}

		// Update with any edition.next changes
		editionDoc.Next.State = instance.State
		if err = s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
			log.ErrorR(r, err, nil)
			handleErrorType(err, w)
			return
		}

		log.Debug("created edition", log.Data{"instance": id, "edition": edition})

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

func updateLinks(instance, currentInstance *models.Instance) *models.InstanceLinks {
	var spatial string
	if instance.Links != nil {
		if instance.Links.Spatial != nil {
			if instance.Links.Spatial.HRef != "" {
				spatial = instance.Links.Spatial.HRef
			}
		}
	}

	links := currentInstance.Links
	if spatial != "" {
		links.Spatial = &models.IDLink{
			HRef: spatial,
		}
	}

	return links
}

func (s *Store) getEdition(datasetID, edition, instanceID string) (*models.EditionUpdate, error) {

	editionDoc, err := s.GetEdition(datasetID, edition, "")
	if err != nil {
		if err != errs.ErrEditionNotFound {
			log.Error(err, nil)
			return nil, err
		}
		// create unique id for edition
		editionID := uuid.NewV4().String()

		editionDoc = &models.EditionUpdate{
			ID: editionID,
			Next: &models.Edition{
				Edition: edition,
				State:   models.EditionConfirmedState,
				Links: &models.EditionUpdateLinks{
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
					LatestVersion: &models.LinkObject{
						ID:   "1",
						HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/1", s.Host, datasetID, edition),
					},
				},
			},
		}
	} else {

		// Update the latest version for the dataset edition
		version, err := strconv.Atoi(editionDoc.Next.Links.LatestVersion.ID)
		if err != nil {
			log.ErrorC("unable to retrieve latest version", err, log.Data{"instance": instanceID, "edition": edition, "version": editionDoc.Next.Links.LatestVersion.ID})
			return nil, err
		}

		version++

		editionDoc.Next.Links.LatestVersion.ID = strconv.Itoa(version)
		editionDoc.Next.Links.LatestVersion.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", s.Host, datasetID, edition, strconv.Itoa(version))
	}

	return editionDoc, nil
}

func validateInstanceUpdate(expectedState string, currentInstance, instance *models.Instance) error {
	var err error
	if currentInstance.State == models.PublishedState {
		err = fmt.Errorf("Unable to update resource state, as the version has been published")
	} else if currentInstance.State != expectedState {
		err = fmt.Errorf("Unable to update resource, expected resource to have a state of %s", expectedState)
	} else if instance.State == models.EditionConfirmedState && currentInstance.Edition == "" && instance.Edition == "" {
		err = errors.New("Unable to update resource, missing a value for the edition")
	}

	return err
}

func (s *Store) defineInstanceLinks(instance *models.Instance, editionDoc *models.EditionUpdate) *models.InstanceLinks {
	stringifiedVersion := strconv.Itoa(instance.Version)

	log.Debug("defining instance links", log.Data{"editionDoc": editionDoc.Next, "instance": instance})

	links := &models.InstanceLinks{
		Dataset: &models.IDLink{
			HRef: editionDoc.Next.Links.Dataset.HRef,
			ID:   editionDoc.Next.Links.Dataset.ID,
		},
		Dimensions: &models.IDLink{
			HRef: fmt.Sprintf("%s/versions/%s/dimensions", editionDoc.Next.Links.Self.HRef, stringifiedVersion),
		},
		Edition: &models.IDLink{
			HRef: editionDoc.Next.Links.Self.HRef,
			ID:   editionDoc.Next.Edition,
		},
		Job: &models.IDLink{
			HRef: instance.Links.Job.HRef,
			ID:   instance.Links.Job.ID,
		},
		Self: &models.IDLink{
			HRef: instance.Links.Self.HRef,
		},
		Version: &models.IDLink{
			HRef: fmt.Sprintf("%s/versions/%s", editionDoc.Next.Links.Self.HRef, stringifiedVersion),
			ID:   stringifiedVersion,
		},
	}

	// Check for spatial link as it is an optional field
	if instance.Links.Spatial != nil {
		links.Spatial = instance.Links.Spatial
	}

	return links
}

// UpdateObservations increments the count of inserted_observations against
// an instance
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

func (s *Store) UpdateImportTask(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	id := vars["id"]

	defer r.Body.Close()

	tasks, err := unmarshalImportTasks(r.Body)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	validationErrs := make([]error, 0)

	if tasks.ImportObservations != nil {
		if tasks.ImportObservations.State != "" {
			if tasks.ImportObservations.State != models.CompletedState {
				validationErrs = append(validationErrs, fmt.Errorf("bad request - invalid task state value for import observations: %v", tasks.ImportObservations.State))
			} else if err := s.UpdateImportObservationsTaskState(id, tasks.ImportObservations.State); err != nil {
				log.Error(err, nil)
				http.Error(w, "Failed to update import observations task state", http.StatusInternalServerError)
				return
			}
		}
	}

	if tasks.BuildHierarchyTasks != nil {
		for _, task := range tasks.BuildHierarchyTasks {
			if task.State != "" {
				if task.State != models.CompletedState {
					validationErrs = append(validationErrs, fmt.Errorf("bad request - invalid task state value: %v", task.State))
				} else if err := s.UpdateBuildHierarchyTaskState(id, task.DimensionName, task.State); err != nil {
					log.Error(err, nil)
					http.Error(w, "Failed to update build hierarchy task state", http.StatusInternalServerError)
					return
				}
			}
		}
	}

	if tasks.BuildSearchIndexTasks != nil {
		for _, task := range tasks.BuildSearchIndexTasks {
			if task.State != "" {
				if task.State != models.CompletedState {
					validationErrs = append(validationErrs, fmt.Errorf("bad request - invalid task state value: %v", task.State))
				} else if err := s.UpdateBuildSearchTaskState(id, task.DimensionName, task.State); err != nil {
					log.Error(err, nil)
					http.Error(w, "Failed to update build search index task state", http.StatusInternalServerError)
					return
				}
			}
		}
	}

	if len(validationErrs) > 0 {
		for _, err := range validationErrs {
			log.Error(err, nil)
		}
		// todo: add all validation errors to the response
		http.Error(w, validationErrs[0].Error(), http.StatusBadRequest)
		return
	}

}

func unmarshalImportTasks(reader io.Reader) (*models.InstanceImportTasks, error) {

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("failed to read message body")
	}

	var tasks models.InstanceImportTasks
	err = json.Unmarshal(b, &tasks)
	if err != nil {
		return nil, errors.New("failed to parse json body: " + err.Error())
	}

	return &tasks, nil
}

func unmarshalInstance(reader io.Reader, post bool) (*models.Instance, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var instance models.Instance
	err = json.Unmarshal(b, &instance)
	if err != nil {
		return nil, errors.New("Failed to parse json body: " + err.Error())
	}

	if instance.State != "" {
		if err := models.ValidateInstanceState(instance.State); err != nil {
			return nil, err
		}
	}

	if post {
		log.Debug("post request on an instance", log.Data{"instance_id": instance.InstanceID})
		if instance.Links == nil || instance.Links.Job == nil {
			return nil, errors.New("Missing job properties")
		}

		// Need both href and id for job link
		if instance.Links.Job.HRef == "" || instance.Links.Job.ID == "" {
			return nil, errors.New("Missing job properties")
		}

		// TODO May want to check the id and href make sense; or change spec to allow
		// for an id only of the dataset and build the href here or vice versa
		// expect an href and strip the job id from the href?

		if instance.State == "" {
			instance.State = models.CreatedState
		}
	}

	return &instance, nil
}

func handleErrorType(err error, w http.ResponseWriter) {
	status := http.StatusInternalServerError

	if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound || err == errs.ErrVersionNotFound || err == errs.ErrDimensionNotFound || err == errs.ErrDimensionNodeNotFound || err == errs.ErrInstanceNotFound {
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

// PublishCheck Checks if an instance has been published
type PublishCheck struct {
	Datastore store.Storer
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		vars := mux.Vars(r)
		id := vars["id"]
		instance, err := d.Datastore.GetInstance(id)
		if err != nil {
			log.Error(err, nil)
			handleErrorType(err, w)
			return
		}

		if instance.State == models.PublishedState {
			err = errors.New("unable to update instance as it has been published")
			log.Error(err, log.Data{"instance": instance})
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		handle(w, r)
	})
}
