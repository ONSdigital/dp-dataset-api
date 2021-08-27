package instance

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// UpdateObservations increments the count of inserted_observations against
// an instance
func (s *Store) UpdateObservations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	insert := vars["inserted_observations"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID, "inserted_observations": insert}

	observations, err := strconv.ParseInt(insert, 10, 64)
	if err != nil {
		log.Error(ctx, "update imported observations: failed to parse inserted_observations string to int", err, logData)
		handleInstanceErr(ctx, errs.ErrInsertedObservationsInvalidSyntax, w, logData)
		return
	}

	// Acquire instance lock to make sure that this call does not interfere with any other 'write' call against the same instance
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
	}
	defer s.UnlockInstance(lockID)

	instance, err := s.GetInstance(instanceID, eTag)
	if err != nil {
		log.Error(ctx, "failed to get instance from database", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	newETag, err := s.UpdateObservationInserted(instance, observations, eTag)
	if err != nil {
		log.Error(ctx, "update imported observations: store.UpdateObservationInserted returned an error", err, logData)
		handleInstanceErr(ctx, err, w, logData)
		return
	}

	log.Info(ctx, "update imported observations: request successful", logData)
	setETag(w, newETag)
}

// UpdateImportTask updates any task in the request body against an instance
func (s *Store) UpdateImportTask(w http.ResponseWriter, r *http.Request) {

	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)
	logData := log.Data{"instance_id": instanceID}
	defer r.Body.Close()

	handleError := func(updateErr *taskError) {
		log.Error(ctx, "updateImportTask endpoint: request unsuccessful", updateErr, logData)
		http.Error(w, updateErr.Error(), updateErr.status)
	}

	tasks, err := unmarshalImportTasks(r.Body)
	if err != nil {
		log.Error(ctx, "failed to unmarshal request body to UpdateImportTasks model", err, logData)
		handleError(&taskError{err, http.StatusBadRequest})
		return
	}

	// Acquire instance lock to make sure that this call does not interfere with any other 'write' call against the same instance
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		handleInstanceErr(ctx, err, w, logData)
	}
	defer s.UnlockInstance(lockID)

	instance, err := s.GetInstance(instanceID, eTag)
	if err != nil {
		log.Error(ctx, "failed to get instance from database", err, logData)
		if err == errs.ErrInstanceConflict {
			handleError(&taskError{err, http.StatusConflict})
			return
		}
		handleError(&taskError{err, http.StatusInternalServerError})
		return
	}

	validationErrs := make([]error, 0)
	var hasImportTasks bool

	if tasks.ImportObservations != nil {
		hasImportTasks = true
		if tasks.ImportObservations.State != "" {
			if tasks.ImportObservations.State != models.CompletedState {
				validationErrs = append(validationErrs, fmt.Errorf("bad request - invalid task state value for import observations: %v", tasks.ImportObservations.State))
			} else {
				eTag, err = s.UpdateImportObservationsTaskState(instance, tasks.ImportObservations.State, eTag)
				if err != nil {
					log.Error(ctx, "failed to update import observations task state", err, logData)
					handleError(&taskError{err, http.StatusInternalServerError})
					return
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
				eTag, err = s.UpdateBuildHierarchyTaskState(instance, task.DimensionName, task.State, eTag)
				if err != nil {
					if err.Error() == errs.ErrNotFound.Error() {
						notFoundErr := task.DimensionName + " hierarchy import task does not exist"
						log.Error(ctx, notFoundErr, err, logData)
						handleError(&taskError{errors.New(notFoundErr), http.StatusNotFound})
						return
					}
					log.Error(ctx, "failed to update build hierarchy task state", err, logData)
					handleError(&taskError{err, http.StatusInternalServerError})
					return
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
				eTag, err = s.UpdateBuildSearchTaskState(instance, task.DimensionName, task.State, eTag)
				if err != nil {
					if err.Error() == "not found" {
						notFoundErr := task.DimensionName + " search index import task does not exist"
						log.Error(ctx, notFoundErr, err, logData)
						handleError(&taskError{errors.New(notFoundErr), http.StatusNotFound})
						return
					}
					log.Error(ctx, "failed to update build hierarchy task state", err, logData)
					handleError(&taskError{err, http.StatusInternalServerError})
					return
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
			log.Error(ctx, "validation error", err, logData)
		}
		// todo: add all validation errors to the response
		handleError(&taskError{validationErrs[0], http.StatusBadRequest})
		return
	}

	log.Info(ctx, "updateImportTask endpoint: request successful", logData)
	setETag(w, eTag)
}

func unmarshalImportTasks(reader io.Reader) (*models.InstanceImportTasks, error) {

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var tasks models.InstanceImportTasks
	if err := json.Unmarshal(b, &tasks); err != nil {
		return nil, err
	}

	return &tasks, nil
}
