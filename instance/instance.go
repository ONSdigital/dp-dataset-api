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
	"github.com/ONSdigital/dp-dataset-api/utils"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

//Store provides a backend for instances
type Store struct {
	store.Storer
	Host                string
	EnableDetachDataset bool
	DefaultOffset       int
	DefaultLimit        int
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

//GetList a list of all instances
func (s *Store) GetList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stateFilterQuery := r.URL.Query().Get("state")
	datasetFilterQuery := r.URL.Query().Get("dataset")
	var stateFilterList []string
	var datasetFilterList []string
	logData := log.Data{}

	if stateFilterQuery != "" {
		logData["state_query"] = stateFilterQuery
		stateFilterList = strings.Split(stateFilterQuery, ",")
	}

	if datasetFilterQuery != "" {
		logData["dataset_query"] = datasetFilterQuery
		datasetFilterList = strings.Split(datasetFilterQuery, ",")
	}

	// get limit from query parameters, or default value
	limit, err := utils.GetPositiveIntQueryParameter(r.URL.Query(), "limit", s.DefaultLimit)
	if err != nil {
		logData["query_params"] = r.URL.RawQuery
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	// get offset from query parameters, or default value
	offset, err := utils.GetPositiveIntQueryParameter(r.URL.Query(), "offset", s.DefaultOffset)
	if err != nil {
		logData["query_params"] = r.URL.RawQuery
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	log.Event(ctx, "get list of instances", log.INFO, logData)

	b, err := func() ([]byte, error) {
		if len(stateFilterList) > 0 {
			if err := models.ValidateStateFilter(stateFilterList); err != nil {
				log.Event(ctx, "get instances: filter state invalid", log.ERROR, log.Error(err), logData)
				return nil, taskError{error: err, status: http.StatusBadRequest}
			}
		}

		results, err := s.GetInstances(ctx, stateFilterList, datasetFilterList, offset, limit)
		if err != nil {
			log.Event(ctx, "get instances: store.GetInstances returned and error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		b, err := json.Marshal(results)
		if err != nil {
			log.Event(ctx, "get instances: failed to marshal results to json", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	writeBody(ctx, w, b, logData)
	log.Event(ctx, "get instances: request successful", log.INFO, logData)
}

//Get a single instance by id
func (s *Store) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	logData := log.Data{"instance_id": instanceID}

	log.Event(ctx, "get instance", log.INFO, logData)

	b, err := func() ([]byte, error) {
		instance, err := s.GetInstance(instanceID)
		if err != nil {
			log.Event(ctx, "get instance: failed to retrieve instance", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		log.Event(ctx, "get instance: checking instance state", log.INFO, logData)
		// Early return if instance state is invalid
		if err = models.CheckState("instance", instance.State); err != nil {
			logData["state"] = instance.State
			log.Event(ctx, "get instance: instance has an invalid state", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		log.Event(ctx, "get instance: marshalling instance json", log.INFO, logData)
		b, err := json.Marshal(instance)
		if err != nil {
			log.Event(ctx, "get instance: failed to marshal instance to json", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	writeBody(ctx, w, b, logData)
	log.Event(ctx, "get instance: request successful", log.INFO, logData)
}

//Add an instance
func (s *Store) Add(w http.ResponseWriter, r *http.Request) {

	defer dphttp.DrainBody(r)

	ctx := r.Context()
	logData := log.Data{}

	log.Event(ctx, "add instance", log.INFO, logData)

	b, err := func() ([]byte, error) {
		instance, err := unmarshalInstance(ctx, r.Body, true)
		if err != nil {
			return nil, err
		}

		logData["instance_id"] = instance.InstanceID

		instance.Links.Self = &models.LinkObject{
			HRef: fmt.Sprintf("%s/instances/%s", s.Host, instance.InstanceID),
		}

		instance, err = s.AddInstance(instance)
		if err != nil {
			log.Event(ctx, "add instance: store.AddInstance returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		b, err := json.Marshal(instance)
		if err != nil {
			log.Event(ctx, "add instance: failed to marshal instance to json", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		return b, nil
	}()
	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	writeBody(ctx, w, b, logData)

	log.Event(ctx, "add instance: request successful", log.INFO, logData)
}

//Update a specific instance
func (s *Store) Update(w http.ResponseWriter, r *http.Request) {

	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	logData := log.Data{"instance_id": instanceID}
	var b []byte
	var err error

	if b, err = func() ([]byte, error) {
		instance, err := unmarshalInstance(ctx, r.Body, false)
		if err != nil {
			log.Event(ctx, "update instance: failed unmarshalling json to model", log.ERROR, log.Error(err), logData)
			return nil, taskError{error: err, status: 400}
		}

		if err = validateInstanceUpdate(instance); err != nil {
			return nil, taskError{error: err, status: 400}
		}

		// Get the current document
		currentInstance, err := s.GetInstance(instanceID)
		if err != nil {
			log.Event(ctx, "update instance: store.GetInstance returned error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		logData["current_state"] = currentInstance.State
		logData["requested_state"] = instance.State
		if instance.State != "" && instance.State != currentInstance.State {
			if err = validateInstanceStateUpdate(instance, currentInstance); err != nil {
				log.Event(ctx, "update instance: instance state invalid", log.ERROR, log.Error(err), logData)
				return nil, err
			}
		}

		datasetID := currentInstance.Links.Dataset.ID

		//edition confirmation is a one time process - cannot be editted for an instance once done
		if instance.State == models.EditionConfirmedState && instance.Version == 0 {
			if instance.Edition == "" {
				instance.Edition = currentInstance.Edition
			}

			edition := instance.Edition
			editionLogData := log.Data{"instance_id": instanceID, "dataset_id": datasetID, "edition": edition}

			editionDoc, editionConfirmErr := s.confirmEdition(ctx, datasetID, edition, instanceID)
			if editionConfirmErr != nil {
				log.Event(ctx, "update instance: store.getEdition returned an error", log.ERROR, log.Error(editionConfirmErr), editionLogData)
				return nil, editionConfirmErr
			}

			//update instance with confirmed edition details
			instance.Links = currentInstance.Links
			instance.Links.Edition = &models.LinkObject{
				ID:   instance.Edition,
				HRef: editionDoc.Next.Links.Self.HRef,
			}

			instance.Links.Version = editionDoc.Next.Links.LatestVersion
			instance.Version, editionConfirmErr = strconv.Atoi(editionDoc.Next.Links.LatestVersion.ID)
			if editionConfirmErr != nil {
				log.Event(ctx, "update instance: failed to convert edition latestVersion id to instance.version int", log.ERROR, log.Error(editionConfirmErr), editionLogData)
				return nil, editionConfirmErr
			}

			if versionErr := s.AddVersionDetailsToInstance(ctx, currentInstance.InstanceID, datasetID, edition, instance.Version); versionErr != nil {
				log.Event(ctx, "update instance: datastore.AddVersionDetailsToInstance returned an error", log.ERROR, log.Error(versionErr), editionLogData)
				return nil, versionErr
			}

			log.Event(ctx, "update instance: added version details to instance", log.INFO, editionLogData)
		}

		// Set the current mongo timestamp on instance document
		instance.UniqueTimestamp = currentInstance.UniqueTimestamp
		if err = s.UpdateInstance(ctx, instanceID, instance); err != nil {
			log.Event(ctx, "update instance: store.UpdateInstance returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if instance, err = s.GetInstance(instanceID); err != nil {
			log.Event(ctx, "update instance: store.GetInstance for response returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		b, err := json.Marshal(instance)
		if err != nil {
			log.Event(ctx, "add instance: failed to marshal instance to json", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		return b, nil
	}(); err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeBody(ctx, w, b, logData)

	log.Event(ctx, "update instance: request successful", log.INFO, logData)
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

		id := uuid.NewV4()

		instance.InstanceID = id.String()
		log.Event(ctx, "post request on an instance", log.INFO, log.Data{"instance_id": instance.InstanceID})
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

func internalError(ctx context.Context, w http.ResponseWriter, err error, logData log.Data) {
	log.Event(ctx, "internal server error", log.ERROR, log.Error(err), logData)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, logData log.Data) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(b); err != nil {
		log.Event(ctx, "failed to write http response body", log.FATAL, log.Error(err), logData)
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
		ctx := r.Context()
		vars := mux.Vars(r)
		instanceID := vars["instance_id"]
		logData := log.Data{"action": "CheckAction", "instance_id": instanceID}

		if err := d.checkState(instanceID); err != nil {
			log.Event(ctx, "errored whilst checking instance state", log.ERROR, log.Error(err), logData)
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
	log.Event(ctx, "request unsuccessful", log.ERROR, log.Error(err), logData)
	http.Error(w, response.Error(), status)
}
