package instance

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

func unmarshalEvent(reader io.Reader) (*models.Event, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var event models.Event
	err = json.Unmarshal(b, &event)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}
	return &event, err
}

//AddEvent details to an instance
func (s *Store) AddEvent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	defer r.Body.Close()

	event, err := unmarshalEvent(r.Body)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = event.Validate(); err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = s.AddEventToInstance(id, event); err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
	w.WriteHeader(http.StatusCreated)
	log.Debug("add event to instance", log.Data{"instance": id})
}
