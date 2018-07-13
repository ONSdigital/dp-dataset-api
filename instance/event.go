package instance

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
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
	data := log.Data{"instance_id": instanceID}
	ap := common.Params{"instance_id": instanceID}

	if err := func() error {
		event, err := unmarshalEvent(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "add instance event: failed to unmarshal request body"), data)
			return err
		}

		if err = event.Validate(); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "add instance event: failed to validate event object"), data)
			return err
		}

		if err = s.AddEventToInstance(instanceID, event); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "add instance event: failed to add event to instance in datastore"), data)
			return err
		}

		return nil
	}(); err != nil {
		if auditErr := s.Auditor.Record(ctx, AddInstanceEventAction, audit.Unsuccessful, ap); auditErr != nil {
			err = auditErr
		}
		handleInstanceErr(ctx, err, w, data)
		return
	}

	if auditErr := s.Auditor.Record(ctx, AddInstanceEventAction, audit.Successful, ap); auditErr != nil {
		handleInstanceErr(ctx, auditErr, w, data)
		return
	}

	log.InfoCtx(ctx, "add instance event: request successful", data)
}
