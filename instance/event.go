package instance

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

// AddInstanceEventAction represents the action to add event
const AddInstanceEventAction = "addInstanceEvent"

func unmarshalEvent(reader io.Reader) (*models.Event, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}
	var event models.Event
	err = json.Unmarshal(b, &event)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
	}
	return &event, nil
}

// AddEvent details to an instance
func (s *Store) AddEvent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()

	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	eTag := getIfMatch(r)
	data := log.Data{"instance_id": instanceID, "action": AddInstanceEventAction}

	event, err := unmarshalEvent(r.Body)
	if err != nil {
		log.Error(ctx, "add instance event: failed to unmarshal request body", err, data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	if err = event.Validate(); err != nil {
		log.Error(ctx, "add instance event: failed to validate event object", err, data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	// Acquire instance lock to make sure that this call does not interfere with any other 'write' call against the same instance
	lockID, err := s.AcquireInstanceLock(ctx, instanceID)
	if err != nil {
		handleInstanceErr(ctx, err, w, data)
	}
	defer s.UnlockInstance(lockID)

	instance, err := s.GetInstance(instanceID, eTag)
	if err != nil {
		log.Error(ctx, "add instance event: failed to get instance from datastore", err, data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	newETag, err := s.AddEventToInstance(instance, event, eTag)
	if err != nil {
		log.Error(ctx, "add instance event: failed to add event to instance in datastore", err, data)
		handleInstanceErr(ctx, err, w, data)
		return
	}

	log.Info(ctx, "add instance event: request successful", data)
	setETag(w, newETag)
}
