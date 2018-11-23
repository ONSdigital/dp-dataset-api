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
	"github.com/ONSdigital/go-ns/request"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

//Store provides a backend for instances
type Store struct {
	store.Storer
	Host    string
	Auditor audit.AuditorService
}

type taskError struct {
	error  error
	status int
}

func (e taskError) Error() string {
	if e.error != nil {
		return e.error.Error()
	}
	return ""
}

// List of audit actions for instances
const (
	AddInstanceAction                = "addInstance"
	CreateEditionAction              = "createEditionForInstance"
	GetInstanceAction                = "getInstance"
	GetInstancesAction               = "getInstances"
	UpdateInstanceAction             = "updateInstance"
	UpdateDimensionAction            = "updateDimension"
	UpdateEditionAction              = "updateEditionNextSubDocForInstance"
	UpdateInsertedObservationsAction = "updateInsertedObservations"
	UpdateImportTasksAction          = "updateImportTasks"
)

//GetList a list of all instances
func (s *Store) GetList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logData := log.Data{}
	stateFilterQuery := r.URL.Query().Get("state")
	datasetFilterQuery := r.URL.Query().Get("dataset")
	var auditParams common.Params
	var stateFilterList []string
	var datasetFilterList []string

	if stateFilterQuery != "" || datasetFilterQuery != "" {
		auditParams = make(common.Params)
	}

	if stateFilterQuery != "" {
		logData["state_query"] = stateFilterQuery
		auditParams["state_query"] = stateFilterQuery
		stateFilterList = strings.Split(stateFilterQuery, ",")
	}

	if datasetFilterQuery != "" {
		logData["dataset_query"] = datasetFilterQuery
		auditParams["dataset_query"] = datasetFilterQuery
		datasetFilterList = strings.Split(datasetFilterQuery, ",")
	}

	log.InfoCtx(ctx, "get list of instances", logData)

	b, err := func() ([]byte, error) {
		if len(stateFilterList) > 0 {
			if err := models.ValidateStateFilter(stateFilterList); err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "get instances: filter state invalid"), logData)
				return nil, taskError{error: err, status: http.StatusBadRequest}
			}
		}

		results, err := s.GetInstances(stateFilterList, datasetFilterList)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "get instances: store.GetInstances returned and error"), nil)
			return nil, err
		}

		b, err := json.Marshal(results)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "get instances: failed to marshal results to json"), nil)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		if auditErr := s.Auditor.Record(ctx, GetInstancesAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	if auditErr := s.Auditor.Record(ctx, GetInstancesAction, audit.Successful, auditParams); auditErr != nil {
		handleInstanceErr(ctx, auditErr, w, logData)
		return
	}

	writeBody(ctx, w, b)
	log.InfoCtx(ctx, "get instances: request successful", logData)
}

//Get a single instance by id
func (s *Store) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	auditParams := common.Params{"instance_id": instanceID}
	logData := audit.ToLogData(auditParams)

	log.InfoCtx(ctx, "get instance", logData)

	b, err := func() ([]byte, error) {
		instance, err := s.GetInstance(instanceID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "get instance: failed to retrieve instance"), logData)
			return nil, err
		}

		log.InfoCtx(ctx, "instance get: checking instance state", logData)
		// Early return if instance state is invalid
		if err = models.CheckState("instance", instance.State); err != nil {
			logData["state"] = instance.State
			log.ErrorCtx(ctx, errors.WithMessage(err, "get instance: instance has an invalid state"), logData)
			return nil, err
		}

		log.InfoCtx(ctx, "instance get: marshalling instance json", logData)
		b, err := json.Marshal(instance)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "get instance: failed to marshal instance to json"), logData)
			return nil, err
		}

		return b, nil
	}()

	log.InfoCtx(ctx, "instance get: auditing outcome", logData)
	if err != nil {
		if auditErr := s.Auditor.Record(ctx, GetInstanceAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	if auditErr := s.Auditor.Record(ctx, GetInstanceAction, audit.Successful, auditParams); auditErr != nil {
		handleInstanceErr(ctx, auditErr, w, logData)
		return
	}

	writeBody(ctx, w, b)
	log.InfoCtx(ctx, "instance get: request successful", logData)
}

//Add an instance
func (s *Store) Add(w http.ResponseWriter, r *http.Request) {

	defer request.DrainBody(r)

	ctx := r.Context()
	logData := log.Data{}
	auditParams := common.Params{}

	log.InfoCtx(ctx, "add instance", logData)

	b, err := func() ([]byte, error) {
		instance, err := unmarshalInstance(ctx, r.Body, true)
		if err != nil {
			return nil, err
		}

		logData["instance_id"] = instance.InstanceID
		auditParams["instance_id"] = instance.InstanceID

		instance.Links.Self = &models.IDLink{
			HRef: fmt.Sprintf("%s/instances/%s", s.Host, instance.InstanceID),
		}

		instance, err = s.AddInstance(instance)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "add instance: store.AddInstance returned an error"), logData)
			return nil, err
		}

		b, err := json.Marshal(instance)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "add instance: failed to marshal instance to json"), logData)
			return nil, err
		}

		return b, nil
	}()
	if err != nil {
		if auditErr := s.Auditor.Record(ctx, AddInstanceAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	s.Auditor.Record(ctx, AddInstanceAction, audit.Successful, auditParams)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	writeBody(ctx, w, b)

	log.InfoCtx(ctx, "add instance: request successful", logData)
}

// UpdateDimension updates label and/or description
// for a specific dimension within an instance
func (s *Store) UpdateDimension(w http.ResponseWriter, r *http.Request) {

	defer request.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	dimension := vars["dimension"]
	auditParams := common.Params{"instance_id": instanceID, "dimension": dimension}
	logData := audit.ToLogData(auditParams)

	log.InfoCtx(ctx, "update instance dimension", logData)

	if err := func() error {
		instance, err := s.GetInstance(instanceID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: Failed to GET instance"), logData)
			return err
		}
		auditParams["instance_state"] = instance.State

		// Early return if instance state is invalid
		if err = models.CheckState("instance", instance.State); err != nil {
			logData["state"] = instance.State
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: current instance has an invalid state"), logData)
			return err
		}

		// Read and unmarshal request body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: error reading request.body"), logData)
			return errs.ErrUnableToReadMessage
		}

		var dim *models.Dimension

		err = json.Unmarshal(b, &dim)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: failing to model models.Codelist resource based on request"), logData)
			return errs.ErrUnableToParseJSON
		}

		// Update instance-dimension
		notFound := true
		for i := range instance.Dimensions {

			// For the chosen dimension
			if instance.Dimensions[i].Name == dimension {
				notFound = false
				// Assign update info, conditionals to allow updating
				// of both or either without blanking other
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
			log.ErrorCtx(ctx, errors.WithMessage(errs.ErrDimensionNotFound, "update instance dimension: dimension not found"), logData)
			return errs.ErrDimensionNotFound
		}

		// Only update dimensions of an instance
		instanceUpdate := &models.Instance{
			Dimensions:      instance.Dimensions,
			UniqueTimestamp: instance.UniqueTimestamp,
		}

		// Update instance
		if err = s.UpdateInstance(ctx, instanceID, instanceUpdate); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update instance dimension: failed to update instance with new dimension label/description"), logData)
			return err
		}

		return nil
	}(); err != nil {
		if auditErr := s.Auditor.Record(ctx, UpdateDimensionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	s.Auditor.Record(ctx, UpdateDimensionAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "updated instance dimension: request successful", logData)
}

//Update a specific instance
func (s *Store) Update(w http.ResponseWriter, r *http.Request) {

	defer request.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	auditParams := common.Params{"instance_id": instanceID}
	logData := audit.ToLogData(auditParams)

	if err := func() error {
		instance, err := unmarshalInstance(ctx, r.Body, false)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: failed unmarshalling json to model"), logData)
			return taskError{error: err, status: 400}
		}

		if err = validateInstanceUpdate(instance); err != nil {
			return taskError{error: err, status: 400}
		}

		// Get the current document
		currentInstance, err := s.GetInstance(instanceID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.GetInstance returned error"), logData)
			return err
		}

		logData["current_state"] = currentInstance.State
		logData["requested_state"] = instance.State
		if instance.State != "" && instance.State != currentInstance.State {
			if err = validateInstanceStateUpdate(instance, currentInstance); err != nil {
				log.ErrorCtx(ctx, errors.Errorf("instance update: instance state invalid"), logData)
				return err
			}
		}

		if instance.State == models.EditionConfirmedState {
			datasetID := currentInstance.Links.Dataset.ID

			// If instance has no edition, get the current edition
			if instance.Edition == "" {
				instance.Edition = currentInstance.Edition
			}
			edition := instance.Edition
			editionAuditParams := common.Params{"instance_id": instanceID, "dataset_id": datasetID, "edition": edition}
			editionLogData := audit.ToLogData(editionAuditParams)

			editionDoc, auditAction, err := func() (*models.EditionUpdate, string, error) {
				// Only create edition if it doesn't already exist
				editionDoc, action, err := s.getEdition(ctx, datasetID, edition, instanceID)
				if err != nil {
					log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.getEdition returned an error"), editionLogData)
					return nil, action, err
				}

				// Update with any edition.next changes
				editionDoc.Next.State = instance.State
				if err = s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
					log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.UpsertEdition returned an error"), editionLogData)
					return nil, action, err
				}

				return editionDoc, action, nil
			}()
			if err != nil {
				if auditAction == "" {
					// No action to audit so return early
					return err
				}

				if auditErr := s.Auditor.Record(ctx, auditAction, audit.Unsuccessful, editionAuditParams); auditErr != nil {
					err = auditErr
				}

				return err
			}

			s.Auditor.Record(ctx, auditAction, audit.Successful, editionAuditParams)

			log.InfoCtx(ctx, "instance update: created edition", editionLogData)

			// Check whether instance has a version
			if currentInstance.Version < 1 {
				// Find the latest version for the dataset edition attached to this
				// instance and append by 1 to set the version of this instance document
				version, err := s.GetNextVersion(datasetID, edition)
				if err != nil {
					log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.GetNextVersion returned an error"), logData)
					return err
				}

				instance.Version = version

				instance = s.addInstanceLinks(ctx, instance, editionDoc)
			}

			if err := s.AddVersionDetailsToInstance(ctx, currentInstance.InstanceID, datasetID, edition, instance.Version); err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: datastore.AddVersionDetailsToInstance returned an error"), logData)
				return err
			}
		}

		// Set the current mongo timestamp on instance document
		instance.UniqueTimestamp = currentInstance.UniqueTimestamp
		if err = s.UpdateInstance(ctx, instanceID, instance); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "instance update: store.UpdateInstance returned an error"), logData)
			return err
		}

		return nil
	}(); err != nil {
		if auditErr := s.Auditor.Record(ctx, UpdateInstanceAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}

		handleInstanceErr(ctx, err, w, logData)
		return
	}

	s.Auditor.Record(ctx, UpdateInstanceAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "instance update: request successful", logData)
}

func validateInstanceUpdate(instance *models.Instance) error {
	var fieldsUnableToUpdate []string
	if instance.Links != nil {
		if instance.Links.Dataset != nil {
			fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Links.Dataset")
		}

		if instance.Links.Dimensions != nil {
			fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Links.Dimensions")
		}

		if instance.Links.Edition != nil {
			fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Links.Edition")
		}

		if instance.Links.Job != nil {
			fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Links.Job")
		}

		if instance.Links.Version != nil {
			fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Links.Version")
		}

		if instance.Links.Self != nil {
			fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Links.Self")
		}
	}

	// Should use events endpoint to update this field
	if instance.Events != nil {
		fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Events")
	}

	// Version number generated by internal application
	if instance.Version > 0 {
		fieldsUnableToUpdate = append(fieldsUnableToUpdate, "instance.Version")
	}

	if len(fieldsUnableToUpdate) > 0 {
		err := fmt.Errorf("unable to update instance contains invalid fields: %s", fieldsUnableToUpdate)
		return err
	}

	return nil
}

func validateInstanceStateUpdate(instance, currentInstance *models.Instance) (err error) {
	if instance.State != "" && instance.State != currentInstance.State {
		switch instance.State {
		case models.SubmittedState:
			if currentInstance.State != models.CreatedState {
				return errs.ErrExpectedResourceStateOfCreated
			}
			break
		case models.CompletedState:
			if currentInstance.State != models.SubmittedState {
				return errs.ErrExpectedResourceStateOfSubmitted
			}
			break
		case models.EditionConfirmedState:
			if currentInstance.State != models.CompletedState {
				return errs.ErrExpectedResourceStateOfCompleted
			}
			break
		case models.AssociatedState:
			if currentInstance.State != models.EditionConfirmedState {
				return errs.ErrExpectedResourceStateOfEditionConfirmed
			}
			break
		case models.PublishedState:
			if currentInstance.State != models.AssociatedState {
				return errs.ErrExpectedResourceStateOfAssociated
			}
			break
		default:
			err = errors.New("instance resource has an invalid state")
			return err
		}
	}

	return nil
}

func (s *Store) getEdition(ctx context.Context, datasetID, edition, instanceID string) (*models.EditionUpdate, string, error) {
	auditParams := common.Params{"dataset_id": datasetID, "instance_id": instanceID, "edition": edition}
	logData := audit.ToLogData(auditParams)

	var action string

	editionDoc, err := s.GetEdition(datasetID, edition, "")
	if err != nil {
		if err != errs.ErrEditionNotFound {
			log.ErrorCtx(ctx, err, logData)
			return nil, action, err
		}

		action = CreateEditionAction
		if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
			return nil, action, auditErr
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
		action = UpdateEditionAction
		if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
			return nil, action, auditErr
		}

		// Update the latest version for the dataset edition
		version, err := strconv.Atoi(editionDoc.Next.Links.LatestVersion.ID)
		if err != nil {
			logData["version"] = editionDoc.Next.Links.LatestVersion.ID
			log.ErrorCtx(ctx, errors.WithMessage(err, "unable to retrieve latest version"), logData)
			return nil, UpdateEditionAction, err
		}

		version++

		editionDoc.Next.Links.LatestVersion.ID = strconv.Itoa(version)
		editionDoc.Next.Links.LatestVersion.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", s.Host, datasetID, edition, strconv.Itoa(version))
	}

	return editionDoc, action, nil
}

func (s *Store) addInstanceLinks(ctx context.Context, instance *models.Instance, editionDoc *models.EditionUpdate) *models.Instance {
	stringifiedVersion := strconv.Itoa(instance.Version)

	log.InfoCtx(ctx, "adding instance links", log.Data{"editionDoc": editionDoc.Next, "instance": instance})

	if instance.Links == nil {
		instance.Links = &models.InstanceLinks{}
	}

	instance.Links.Dataset = &models.IDLink{
		HRef: editionDoc.Next.Links.Dataset.HRef,
		ID:   editionDoc.Next.Links.Dataset.ID,
	}

	instance.Links.Dimensions = &models.IDLink{
		HRef: fmt.Sprintf("%s/versions/%s/dimensions", editionDoc.Next.Links.Self.HRef, stringifiedVersion),
	}

	instance.Links.Edition = &models.IDLink{
		HRef: editionDoc.Next.Links.Self.HRef,
		ID:   editionDoc.Next.Edition,
	}

	instance.Links.Version = &models.IDLink{
		HRef: fmt.Sprintf("%s/versions/%s", editionDoc.Next.Links.Self.HRef, stringifiedVersion),
		ID:   stringifiedVersion,
	}

	return instance
}

// UpdateObservations increments the count of inserted_observations against
// an instance
func (s *Store) UpdateObservations(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	insert := vars["inserted_observations"]
	auditParams := common.Params{"instance_id": instanceID, "inserted_observations": insert}
	logData := audit.ToLogData(auditParams)

	if err := func() error {
		observations, err := strconv.ParseInt(insert, 10, 64)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update imported observations: failed to parse inserted_observations string to int"), logData)
			return errs.ErrInsertedObservationsInvalidSyntax
		}

		if err = s.UpdateObservationInserted(instanceID, observations); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "update imported observations: store.UpdateObservationInserted returned an error"), logData)
			return err
		}

		return nil
	}(); err != nil {
		if auditErr := s.Auditor.Record(ctx, UpdateInsertedObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	s.Auditor.Record(ctx, UpdateInsertedObservationsAction, audit.Successful, auditParams)

	log.InfoCtx(ctx, "update imported observations: request successful", logData)
}

// UpdateImportTask updates any task in the request body against an instance
func (s *Store) UpdateImportTask(w http.ResponseWriter, r *http.Request) {

	defer request.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	auditParams := common.Params{"instance_id": instanceID}
	logData := audit.ToLogData(auditParams)
	defer r.Body.Close()

	updateErr := func() *taskError {
		tasks, err := unmarshalImportTasks(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to unmarshal request body to UpdateImportTasks model"), logData)
			return &taskError{err, http.StatusBadRequest}
		}

		validationErrs := make([]error, 0)
		var hasImportTasks bool

		if tasks.ImportObservations != nil {
			hasImportTasks = true
			if tasks.ImportObservations.State != "" {
				if tasks.ImportObservations.State != models.CompletedState {
					validationErrs = append(validationErrs, fmt.Errorf("bad request - invalid task state value for import observations: %v", tasks.ImportObservations.State))
				} else {
					if err := s.UpdateImportObservationsTaskState(instanceID, tasks.ImportObservations.State); err != nil {
						log.ErrorCtx(ctx, errors.WithMessage(err, "Failed to update import observations task state"), logData)
						return &taskError{err, http.StatusInternalServerError}
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
					if err := s.UpdateBuildHierarchyTaskState(instanceID, task.DimensionName, task.State); err != nil {
						if err.Error() == "not found" {
							notFoundErr := task.DimensionName + " hierarchy import task does not exist"
							log.ErrorCtx(ctx, errors.WithMessage(err, notFoundErr), logData)
							return &taskError{errors.New(notFoundErr), http.StatusNotFound}
						}
						log.ErrorCtx(ctx, errors.WithMessage(err, "failed to update build hierarchy task state"), logData)
						return &taskError{err, http.StatusInternalServerError}
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
					if err := s.UpdateBuildSearchTaskState(instanceID, task.DimensionName, task.State); err != nil {
						if err.Error() == "not found" {
							notFoundErr := task.DimensionName + " search index import task does not exist"
							log.ErrorCtx(ctx, errors.WithMessage(err, notFoundErr), logData)
							return &taskError{errors.New(notFoundErr), http.StatusNotFound}
						}
						log.ErrorCtx(ctx, errors.WithMessage(err, "failed to update build hierarchy task state"), logData)
						return &taskError{err, http.StatusInternalServerError}
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
				log.ErrorCtx(ctx, errors.WithMessage(err, "validation error"), logData)
			}
			// todo: add all validation errors to the response
			return &taskError{validationErrs[0], http.StatusBadRequest}
		}
		return nil
	}()

	if updateErr != nil {
		if auditErr := s.Auditor.Record(ctx, UpdateImportTasksAction, audit.Unsuccessful, auditParams); auditErr != nil {
			updateErr = &taskError{errs.ErrInternalServer, http.StatusInternalServerError}
		}
		log.ErrorCtx(ctx, errors.WithMessage(updateErr, "updateImportTask endpoint: request unsuccessful"), logData)
		http.Error(w, updateErr.Error(), updateErr.status)
		return
	}

	if auditErr := s.Auditor.Record(ctx, UpdateImportTasksAction, audit.Successful, auditParams); auditErr != nil {
		return
	}

	log.InfoCtx(ctx, "updateImportTask endpoint: request successful", logData)
}

func unmarshalImportTasks(reader io.Reader) (*models.InstanceImportTasks, error) {

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var tasks models.InstanceImportTasks
	err = json.Unmarshal(b, &tasks)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
	}

	return &tasks, nil
}

func unmarshalInstance(ctx context.Context, reader io.Reader, post bool) (*models.Instance, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var instance models.Instance
	err = json.Unmarshal(b, &instance)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
	}

	if instance.State != "" {
		if err := models.ValidateInstanceState(instance.State); err != nil {
			return nil, err
		}
	}

	if post {
		// TODO Should validate against fields that will be auto generated internally
		// as these should not be allowed to be added to resource, (for example link.self,
		// version, links.version, last_updated, id, downloads, events to name a few).
		// One could use a different model, so when unmarshalling request body into an
		// instance object, it will not include those fields.

		instance.InstanceID = uuid.NewV4().String()
		log.InfoCtx(ctx, "post request on an instance", log.Data{"instance_id": instance.InstanceID})
		if instance.Links == nil || instance.Links.Job == nil {
			return nil, errs.ErrMissingJobProperties
		}

		// Need both href and id for job link
		if instance.Links.Job.HRef == "" || instance.Links.Job.ID == "" {
			return nil, errs.ErrMissingJobProperties
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

func internalError(ctx context.Context, w http.ResponseWriter, err error) {
	log.ErrorCtx(ctx, err, nil)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		log.ErrorCtx(ctx, err, nil)
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
		instanceID := vars["instance_id"]
		logData := log.Data{"instance_id": instanceID}
		auditParams := common.Params{"instance_id": instanceID}

		if vars["dimension"] != "" {
			auditParams["dimension"] = vars["dimension"]
		}

		if err := d.checkState(instanceID, logData, auditParams); err != nil {
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

func (d *PublishCheck) checkState(instanceID string, logData log.Data, auditParams common.Params) error {
	instance, err := d.Datastore.GetInstance(instanceID)
	if err != nil {
		return err
	}
	auditParams["instance_state"] = instance.State

	if instance.State == models.PublishedState {
		return errs.ErrResourcePublished
	}

	if err = models.ValidateInstanceState(instance.State); err != nil {
		return errs.ErrInternalServer
	}

	return nil
}

func handleInstanceErr(ctx context.Context, err error, w http.ResponseWriter, logData log.Data) {
	if logData == nil {
		logData = log.Data{}
	}

	taskErr, isTaskErr := err.(taskError)

	var status int
	response := err

	switch {
	case isTaskErr:
		status = taskErr.status
	case errs.NotFoundMap[err]:
		status = http.StatusNotFound
	case errs.BadRequestMap[err]:
		status = http.StatusBadRequest
	case errs.ForbiddenMap[err]:
		status = http.StatusForbidden
	case errs.ConflictRequestMap[err]:
		status = http.StatusConflict
	default:
		status = http.StatusInternalServerError
		response = errs.ErrInternalServer
	}

	logData["responseStatus"] = status
	log.ErrorCtx(ctx, errors.WithMessage(err, "request unsuccessful"), logData)
	http.Error(w, response.Error(), status)
}
