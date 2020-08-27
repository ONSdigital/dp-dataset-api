package instance

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

// AddInstanceEventAction represents the audit action to add event
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

//AddEvent details to an instance
func (s *Store) AddEvent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()

	vars := mux.Vars(r)
	instanceID := vars["instance_id"]
	data := log.Data{"instance_id": instanceID, "action": AddInstanceEventAction}

	if err := func() error {
		event, err := unmarshalEvent(r.Body)
		if err != nil {
			log.Event(ctx, "add instance event: failed to unmarshal request body", log.ERROR, log.Error(err), data)
			return err
		}

		if err = event.Validate(); err != nil {
			log.Event(ctx, "add instance event: failed to validate event object", log.ERROR, log.Error(err), data)
			return err
		}

		if err = s.AddEventToInstance(instanceID, event); err != nil {
			log.Event(ctx, "add instance event: failed to add event to instance in datastore", log.ERROR, log.Error(err), data)
			return err
		}

		return nil
	}(); err != nil {
		handleInstanceErr(ctx, err, w, data)
		return
	}

	log.Event(ctx, "add instance event: request successful", log.INFO, data)
}
