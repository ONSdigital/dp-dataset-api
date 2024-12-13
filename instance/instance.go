package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/store"
	dpresponse "github.com/ONSdigital/dp-net/v2/handlers/response"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Store provides a backend for instances
type Store struct {
	store.Storer
	Host                string
	EnableDetachDataset bool
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

// GetList returns a list of instances, the total count of instances that match the query parameters and an error
func (s *Store) GetList(w http.ResponseWriter, r *http.Request, limit, offset int) (results interface{}, totalCount int, err error) {
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

	log.Info(ctx, "get list of instances", logData)

	results, totalCount, err = func() ([]*models.Instance, int, error) {
		if len(stateFilterList) > 0 {
			if err := models.ValidateStateFilter(stateFilterList); err != nil {
				log.Error(ctx, "get instances: filter state invalid", err, logData)
				return nil, 0, taskError{error: err, status: http.StatusBadRequest}
			}
		}

		instancesResults, instancesTotalCount, err := s.GetInstances(ctx, stateFilterList, datasetFilterList, offset, limit)
		if err != nil {
			log.Error(ctx, "get instances: store.GetInstances returned an error", err, logData)
			return nil, 0, err
		}

		return instancesResults, instancesTotalCount, nil
	}()

	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return nil, 0, err
	}

	log.Info(ctx, "get instances: request successful", logData)
	return results, totalCount, nil
}

// Get a single instance by id
// Note that this method doesn't need to acquire the instance lock because it's a getter,
// which will fail if the ETag doesn't match, and cannot interfere with writers.
func (s *Store) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID}

	log.Info(ctx, "get instance", logData)

	instance, err := s.GetInstance(ctx, instanceID, eTag)
	if err != nil {
		log.Error(ctx, "get instance: failed to retrieve instance", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	log.Info(ctx, "get instance: checking instance state", logData)
	// Early return if instance state is invalid
	if err = models.CheckState("instance", instance.State); err != nil {
		logData["state"] = instance.State
		log.Error(ctx, "get instance: instance has an invalid state", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	log.Info(ctx, "get instance: marshalling instance json", logData)
	b, err := json.Marshal(instance)
	if err != nil {
		log.Error(ctx, "get instance: failed to marshal instance to json", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	dpresponse.SetETag(w, instance.ETag)
	writeBody(ctx, w, b, logData)
	log.Info(ctx, "get instance: request successful", logData)
}

// Add an instance
// Note that this method doesn't need to acquire the instance lock because it creates a new instance,
// so it is not possible that any other call is concurrently trying to access the same instance
func (s *Store) Add(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	logData := log.Data{}

	log.Info(ctx, "add instance", logData)

	instance, err := unmarshalInstance(ctx, r.Body, true)
	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	logData["instance_id"] = instance.InstanceID

	instance.Links.Self = &models.LinkObject{
		HRef: fmt.Sprintf("%s/instances/%s", s.Host, instance.InstanceID),
	}

	instance, err = s.AddInstance(ctx, instance)
	if err != nil {
		log.Error(ctx, "add instance: store.AddInstance returned an error", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	b, err := json.Marshal(instance)
	if err != nil {
		log.Error(ctx, "add instance: failed to marshal instance to json", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	dpresponse.SetETag(w, instance.ETag)
	w.WriteHeader(http.StatusCreated)
	writeBody(ctx, w, b, logData)

	log.Info(ctx, "add instance: request successful", logData)
}

// Update a specific instance
// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (s *Store) Update(w http.ResponseWriter, r *http.Request) {
	// We don't set up the: "defer dphttp.DrainBody(r)" here, as the body is fully read in function unmarshalInstance() below
	// and a call to DrainBody() puts this error: "invalid Read on closed Body" into the logs - to no good effect
	// because there is no more body to be read - so instead we just set up the usual Close() on the Body.
	defer func() {
		if bodyCloseErr := r.Body.Close(); bodyCloseErr != nil {
			log.Error(r.Context(), "could not close response body", bodyCloseErr)
		}
	}()

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)

	logData := log.Data{"instance_id": instanceID}

	instance, err := unmarshalInstance(ctx, r.Body, false)
	if err != nil {
		log.Error(ctx, "update instance: failed unmarshalling json to model", err, logData)
		handleInstanceErr(ctx, taskError{error: err, status: 400}, w, logData)
		return
	}

	if err = validateInstanceUpdate(instance); err != nil {
		handleInstanceErr(ctx, taskError{error: err, status: 400}, w, logData)
		return
	}

	// acquire instance lock so that the dp-graph call to AddVersionDetailsToInstance and the mongoDB update are atomic
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		log.Error(ctx, "update instance: failed to lock the instance", err, logData)
		handleInstanceErr(ctx, taskError{error: err, status: http.StatusInternalServerError}, w, logData)
		return
	}
	defer s.UnlockInstance(ctx, lockID)

	// Get the current document
	currentInstance, err := s.GetInstance(ctx, instanceID, eTag)
	if err != nil {
		log.Error(ctx, "update instance: store.GetInstance returned error", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	logData["current_state"] = currentInstance.State
	logData["requested_state"] = instance.State
	if instance.State != "" && instance.State != currentInstance.State {
		if err = validateInstanceStateUpdate(instance, currentInstance); err != nil {
			log.Error(ctx, "update instance: instance state invalid", err, logData)
			handleInstanceErr(ctx, err, w, logData)
			return
		}
	}

	datasetID := currentInstance.Links.Dataset.ID

	// edition confirmation is a one time process - cannot be edited for an instance once done
	if instance.State == models.EditionConfirmedState && instance.Version == 0 {
		if instance.Edition == "" {
			instance.Edition = currentInstance.Edition
		}

		edition := instance.Edition
		editionLogData := log.Data{"instance_id": instanceID, "dataset_id": datasetID, "edition": edition}

		editionDoc, editionConfirmErr := s.confirmEdition(ctx, datasetID, edition, instanceID)
		if editionConfirmErr != nil {
			log.Error(ctx, "update instance: store.getEdition returned an error", editionConfirmErr, editionLogData)
			handleInstanceErr(ctx, editionConfirmErr, w, logData)
			return
		}

		// update instance with confirmed edition details
		instance.Links = currentInstance.Links
		instance.Links.Edition = &models.LinkObject{
			ID:   instance.Edition,
			HRef: editionDoc.Next.Links.Self.HRef,
		}

		instance.Links.Version = editionDoc.Next.Links.LatestVersion
		instance.Version, editionConfirmErr = strconv.Atoi(editionDoc.Next.Links.LatestVersion.ID)
		if editionConfirmErr != nil {
			log.Error(ctx, "update instance: failed to convert edition latestVersion id to instance.version int", editionConfirmErr, editionLogData)
			handleInstanceErr(ctx, editionConfirmErr, w, logData)
			return
		}

		// update dp-graph instance node (only for non-cantabular types)
		if currentInstance.Type == models.CantabularBlob.String() || currentInstance.Type == models.CantabularTable.String() || currentInstance.Type == models.CantabularFlexibleTable.String() || currentInstance.Type == models.CantabularMultivariateTable.String() {
			editionLogData["instance_type"] = instance.Type
			log.Info(ctx, "skipping dp-graph instance update because it is not required by instance type", editionLogData)
		}
		// } else {

		// 	if versionErr := s.AddVersionDetailsToInstance(ctx, currentInstance.InstanceID, datasetID, edition, instance.Version); versionErr != nil {
		// 		log.Error(ctx, "update instance: datastore.AddVersionDetailsToInstance returned an error", versionErr, editionLogData)
		// 		handleInstanceErr(ctx, versionErr, w, logData)
		// 		return
		// 	}

		// }

		log.Info(ctx, "update instance: added version details to instance", editionLogData)
	}

	// Set the current mongo timestamp on instance document
	instance.UniqueTimestamp = currentInstance.UniqueTimestamp
	newETag, err := s.UpdateInstance(ctx, currentInstance, instance, eTag)
	if err != nil {
		log.Error(ctx, "update instance: store.UpdateInstance returned an error", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	if instance, err = s.GetInstance(ctx, instanceID, newETag); err != nil {
		log.Error(ctx, "update instance: store.GetInstance for response returned an error", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	b, err := json.Marshal(instance)
	if err != nil {
		log.Error(ctx, "add instance: failed to marshal instance to json", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	dpresponse.SetETag(w, newETag)
	w.WriteHeader(http.StatusOK)
	writeBody(ctx, w, b, logData)

	log.Info(ctx, "update instance: request successful", logData)
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
		case models.CompletedState:
			if currentInstance.State != models.SubmittedState {
				return errs.ErrExpectedResourceStateOfSubmitted
			}
		case models.EditionConfirmedState:
			if currentInstance.State != models.CompletedState {
				return errs.ErrExpectedResourceStateOfCompleted
			}
		case models.AssociatedState:
			if currentInstance.State != models.EditionConfirmedState {
				return errs.ErrExpectedResourceStateOfEditionConfirmed
			}
		case models.PublishedState:
			if currentInstance.State != models.AssociatedState {
				return errs.ErrExpectedResourceStateOfAssociated
			}
		case models.FailedState:
			break
		default:
			err = errors.New("instance resource has an invalid state")
			return err
		}
	}

	return nil
}

func unmarshalInstance(ctx context.Context, reader io.Reader, post bool) (*models.Instance, error) {
	b, err := io.ReadAll(reader)
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

		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}

		instance.InstanceID = id.String()
		log.Info(ctx, "post request on an instance", log.Data{"instance_id": instance.InstanceID})
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

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func getIfMatch(r *http.Request) string {
	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		return mongo.AnyETag
	}
	return ifMatch
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, logData log.Data) {
	if _, err := w.Write(b); err != nil {
		log.Error(ctx, "failed to write http response body", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// PublishCheck Checks if an instance has been published
type PublishCheck struct {
	Datastore store.Storer
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		vars := mux.Vars(r)
		instanceID := vars["instance_id"]
		eTag := getIfMatch(r)
		logData := log.Data{"action": "CheckAction", "instance_id": instanceID}

		if err := d.checkState(ctx, instanceID, eTag); err != nil {
			log.Error(ctx, "errored whilst checking instance state", err, logData)
			handleInstanceErr(ctx, err, w, logData)
			return
		}

		handle(w, r)
	}
}

func (d *PublishCheck) checkState(ctx context.Context, instanceID, eTagSelector string) error {
	instance, err := d.Datastore.GetInstance(ctx, instanceID, eTagSelector)
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
	log.Error(ctx, "request unsuccessful", err, logData)
	http.Error(w, response.Error(), status)
}
