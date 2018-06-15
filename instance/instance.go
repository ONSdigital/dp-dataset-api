package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

var updateImportTaskAction = "updateImportTask"

//Store provides a backend for instances
type Store struct {
	store.Storer
	Host    string
	Auditor audit.AuditorService
}

type updateTaskErr struct {
	error  error
	status int
}

func (e updateTaskErr) Error() string {
	if e.error != nil {
		return e.error.Error()
	}
	return ""
}

// List of audit actions for instances
const (
	PutInstanceAction       = "putInstance"
	PutDimensionAction      = "putDimension"
	PutInsertedObservations = "putInsertedObservations"
	PutImportTasks          = "putImportTasks"
)

//GetList a list of all instances
func (s *Store) GetList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stateFilterQuery := r.URL.Query().Get("state")
	var stateFilterList []string
	if stateFilterQuery != "" {
		stateFilterList = strings.Split(stateFilterQuery, ",")
		if err := models.ValidateStateFilter(stateFilterList); err != nil {
			log.ErrorCtx(ctx, err, nil)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	results, err := s.GetInstances(stateFilterList)
	if err != nil {
		log.ErrorCtx(ctx, err, nil)
		handleInstanceErr(ctx, err, w, nil)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		internalError(w, err)
		return
	}

	writeBody(w, b)
	log.InfoCtx(ctx, "instance getList: request successful", log.Data{"query": stateFilterQuery})
}

//Get a single instance by id
func (s *Store) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	data := log.Data{"instance_id": id}

	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorCtx(ctx, err, data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		data["state"] = instance.State
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance get: instance has an invalid state"), data)
		internalError(w, err)
		return
	}

	b, err := json.Marshal(instance)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "failed to marshal instance to json"), data)
		internalError(w, err)
		return
	}

	writeBody(w, b)
	log.InfoCtx(ctx, "instance get: request successful", data)
}

//Add an instance
func (s *Store) Add(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ctx := r.Context()
	instance, err := unmarshalInstance(r.Body, true)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance add: failed to unmarshal json to model"), nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	instance.InstanceID = uuid.NewV4().String()
	data := log.Data{"instance_id": instance.InstanceID}

	instance.Links.Self = &models.IDLink{
		HRef: fmt.Sprintf("%s/instances/%s", s.Host, instance.InstanceID),
	}

	instance, err = s.AddInstance(instance)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance add: store.AddInstance returned an error"), data)
		internalError(w, err)
		return
	}

	b, err := json.Marshal(instance)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance add: failed to marshal instance to json"), data)
		internalError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	writeBody(w, b)
	log.InfoCtx(ctx, "instance add: request successful", data)
}

// UpdateDimension updates label and/or description for a specific dimension within an instance
func (s *Store) UpdateDimension(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	dimension := vars["dimension"]
	data := log.Data{"instance_id": id, "dimension": dimension}

	// Get instance
	instance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update dimension: Failed to GET instance"), data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		data["state"] = instance.State
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update dimension: current instance has an invalid state"), data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	// Early return if instance is already published
	if instance.State == models.PublishedState {
		log.InfoCtx(ctx, "instance update dimension: unable to update instance/version, already published", data)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Read and unmarshal request body
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update dimension: error reading request.body"), data)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var dim *models.CodeList

	err = json.Unmarshal(b, &dim)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update dimension: failing to model models.Codelist resource based on request"), data)
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
		log.ErrorCtx(ctx, errors.WithMessage(errs.ErrDimensionNotFound, "instance update dimension: dimension not found"), data)
		handleInstanceErr(ctx, errs.ErrDimensionNotFound, w, data)
		return
	}

	// Update instance
	if err = s.UpdateInstance(id, instance); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update dimension: failed to update instance with new dimension label/description"), data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	log.InfoCtx(ctx, "instance updated dimension: request successful", data)
}

//Update a specific instance
func (s *Store) Update(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	data := log.Data{"instance_id": id}
	defer r.Body.Close()

	instance, err := unmarshalInstance(r.Body, false)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: failed unmarshalling json to model"), data)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get the current document
	currentInstance, err := s.GetInstance(id)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.GetInstance returned error"), data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	// Early return if instance state is invalid
	if err = models.CheckState("instance", currentInstance.State); err != nil {
		data["state"] = currentInstance.State
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: current instance has an invalid state"), data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	// Combine existing links and spatial link
	instance.Links = updateLinks(instance, currentInstance)

	logData := log.Data{"instance_id": id, "current_state": currentInstance.State, "requested_state": instance.State}
	if instance.State != currentInstance.State {
		var expectedState string

		switch instance.State {
		case models.CompletedState:
			expectedState = models.SubmittedState
		case models.EditionConfirmedState:
			expectedState = models.CompletedState
		case models.AssociatedState:
			expectedState = models.EditionConfirmedState
		case models.PublishedState:
			expectedState = models.AssociatedState
		default:
			log.ErrorCtx(ctx, errors.Errorf("instance update: instance state invalid"), logData)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err = validateInstanceUpdate(expectedState, currentInstance, instance); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: instance validation failure"), logData)
			http.Error(w, err.Error(), http.StatusForbidden)
			return
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
		editionDoc, err := s.getEdition(ctx, datasetID, edition, id)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.getEdition returned an error"), data)
			handleInstanceErr(ctx, err, w, data)
			return
		}

		// Update with any edition.next changes
		editionDoc.Next.State = instance.State
		if err = s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.UpsertEdition returned an error"), data)
			handleInstanceErr(ctx, err, w, data)
			return
		}

		data["edition"] = edition
		log.InfoCtx(ctx, "instance update: created edition", data)

		// Check whether instance has a version
		if currentInstance.Version < 1 {
			// Find the latest version for the dataset edition attached to this
			// instance and append by 1 to set the version of this instance document
			version, err := s.GetNextVersion(datasetID, edition)
			if err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.GetNextVersion returned an error"), data)
				handleInstanceErr(ctx, err, w, data)
				return
			}

			instance.Version = version

			links := s.defineInstanceLinks(instance, editionDoc)
			instance.Links = links
		}
	}

	if err = s.UpdateInstance(id, instance); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.UpdateInstance returned an error"), data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	log.InfoCtx(ctx, "instance update: request successful", data)
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

func (s *Store) getEdition(ctx context.Context, datasetID, edition, instanceID string) (*models.EditionUpdate, error) {
	data := log.Data{"dataset_id": datasetID, "instance_id": instanceID, "edition": edition}
	editionDoc, err := s.GetEdition(datasetID, edition, "")
	if err != nil {
		if err != errs.ErrEditionNotFound {
			log.ErrorCtx(ctx, err, data)
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
			data["version"] = editionDoc.Next.Links.LatestVersion.ID
			log.ErrorCtx(ctx, errors.WithMessage(err, "unable to retrieve latest version"), data)
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
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	insert := vars["inserted_observations"]

	observations, err := strconv.ParseInt(insert, 10, 64)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "update observations: failed to parse inserted_observations string to int"), log.Data{"stringValue": insert})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = s.UpdateObservationInserted(id, observations); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "update observations: store.UpdateObservationInserted returned an error"), log.Data{"id": id})
		handleInstanceErr(ctx, err, w, nil)
	}
}

// UpdateImportTask updates any task in the request body against an instance
func (s *Store) UpdateImportTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	ap := common.Params{"ID": id}
	data := audit.ToLogData(ap)
	defer r.Body.Close()

	if auditErr := s.Auditor.Record(ctx, updateImportTaskAction, audit.Attempted, ap); auditErr != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	updateErr := func() *updateTaskErr {
		tasks, err := unmarshalImportTasks(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to unmarshal request body to UpdateImportTasks model"), data)
			return &updateTaskErr{err, http.StatusBadRequest}
		}

		validationErrs := make([]error, 0)
		var hasImportTasks bool

		if tasks.ImportObservations != nil {
			hasImportTasks = true
			if tasks.ImportObservations.State != "" {
				if tasks.ImportObservations.State != models.CompletedState {
					validationErrs = append(validationErrs, fmt.Errorf("bad request - invalid task state value for import observations: %v", tasks.ImportObservations.State))
				} else {
					if err := s.UpdateImportObservationsTaskState(id, tasks.ImportObservations.State); err != nil {
						log.ErrorCtx(ctx, errors.WithMessage(err, "Failed to update import observations task state"), data)
						return &updateTaskErr{err, http.StatusInternalServerError}
					}
				}
			} else {
				validationErrs = append(validationErrs, errors.New("bad request - invalid import observation task, must include state"))
			}
		}

		if tasks.BuildHierarchyTasks != nil {
			hasImportTasks = true
			var hasHierarchyImportTask bool
			for _, task := range tasks.BuildHierarchyTasks {
				hasHierarchyImportTask = true
				if err := models.ValidateImportTask(task.GenericTaskDetails); err != nil {
					validationErrs = append(validationErrs, err)
				} else {
					if err := s.UpdateBuildHierarchyTaskState(id, task.DimensionName, task.State); err != nil {
						if err.Error() == "not found" {
							notFoundErr := task.DimensionName + " hierarchy import task does not exist"
							log.ErrorCtx(ctx, errors.WithMessage(err, notFoundErr), data)
							return &updateTaskErr{errors.New(notFoundErr), http.StatusNotFound}
						}
						log.ErrorCtx(ctx, errors.WithMessage(err, "failed to update build hierarchy task state"), data)
						return &updateTaskErr{err, http.StatusInternalServerError}
					}
				}
			}
			if !hasHierarchyImportTask {
				validationErrs = append(validationErrs, errors.New("bad request - missing hierarchy task"))
			}
		}

		if tasks.BuildSearchIndexTasks != nil {
			hasImportTasks = true
			var hasSearchIndexImportTask bool
			for _, task := range tasks.BuildSearchIndexTasks {
				hasSearchIndexImportTask = true
				if err := models.ValidateImportTask(task.GenericTaskDetails); err != nil {
					validationErrs = append(validationErrs, err)
				} else {
					if err := s.UpdateBuildSearchTaskState(id, task.DimensionName, task.State); err != nil {
						log.Error(err, nil)
						if err.Error() == "not found" {
							notFoundErr := task.DimensionName + " search index import task does not exist"
							log.ErrorCtx(ctx, errors.WithMessage(err, notFoundErr), data)
							return &updateTaskErr{errors.New(notFoundErr), http.StatusNotFound}
						}
						log.ErrorCtx(ctx, errors.WithMessage(err, "failed to update build hierarchy task state"), data)
						return &updateTaskErr{err, http.StatusInternalServerError}
					}
				}
			}
			if !hasSearchIndexImportTask {
				validationErrs = append(validationErrs, errors.New("bad request - missing search index task"))
			}
		}

		if !hasImportTasks {
			validationErrs = append(validationErrs, errors.New("bad request - request body does not contain any import tasks"))
		}

		if len(validationErrs) > 0 {
			for _, err := range validationErrs {
				log.ErrorCtx(ctx, errors.WithMessage(err, "validation error"), data)
			}
			// todo: add all validation errors to the response
			return &updateTaskErr{validationErrs[0], http.StatusBadRequest}
		}
		return nil
	}()

	if updateErr != nil {
		if auditErr := s.Auditor.Record(ctx, updateImportTaskAction, audit.Unsuccessful, ap); auditErr != nil {
			updateErr = &updateTaskErr{errs.ErrInternalServer, http.StatusInternalServerError}
		}
		log.ErrorCtx(ctx, errors.WithMessage(updateErr, "updateImportTask endpoint: request unsuccessful"), data)
		http.Error(w, updateErr.Error(), updateErr.status)
		return
	}

	if auditErr := s.Auditor.Record(ctx, updateImportTaskAction, audit.Successful, ap); auditErr != nil {
		return
	}

	log.InfoCtx(ctx, "updateImportTask endpoint: request successful", data)
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
	Auditor   audit.AuditorService
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request), action string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		vars := mux.Vars(r)
		instanceID := vars["id"]
		logData := log.Data{"instance_id": instanceID}
		auditParams := common.Params{"instance_id": instanceID}

		if err := d.Auditor.Record(ctx, action, audit.Attempted, auditParams); err != nil {
			handleInstanceErr(ctx, errs.ErrAuditActionAttemptedFailure, w, logData)
			return
		}

		if err := d.checkState(instanceID); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "errored whilst checking instance state"), logData)
			if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
				handleInstanceErr(ctx, errs.ErrAuditActionAttemptedFailure, w, logData)
				return
			}

			handleInstanceErr(ctx, err, w, logData)
			return
		}

		handle(w, r)
	})
}

func (d *PublishCheck) checkState(instanceID string) error {
	instance, err := d.Datastore.GetInstance(instanceID)
	if err != nil {
		return err
	}

	if instance.State == models.PublishedState {
		return errs.ErrResourcePublished
	}

	return nil
}

func handleInstanceErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int
	response := err

	switch {
	case errs.NotFoundMap[err]:
		status = http.StatusNotFound
	case err == errs.ErrResourcePublished:
		status = http.StatusForbidden
	default:
		status = http.StatusInternalServerError
		response = errs.ErrInternalServer
	}

	data["responseStatus"] = status
	log.ErrorCtx(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, response.Error(), status)
}
