package models

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"time"
)

// Instance which presents a single dataset being imported
type Instance struct {
	InstanceID           string    `bson:"id,omitempty"                          json:"id,omitempty"`
	Job                  IDLink    `bson:"job,omitempty"                         json:"job,omitempty"`
	State                string    `bson:"state,omitempty"                       json:"state,omitempty"`
	Events               *[]Event  `bson:"events,omitempty"                      json:"events,omitempty"`
	TotalObservations    *int      `bson:"total_observations,omitempty"          json:"total_observations,omitempty"`
	InsertedObservations *int      `bson:"total_inserted_observations,omitempty" json:"total_inserted_observations,omitempty"`
	Headers              *[]string `bson:"headers,omitempty"                     json:"headers,omitempty"`
	LastUpdated          time.Time `bson:"last_updated,omitempty"                json:"last_updated,omitempty"`
}

// IDLink holds the id and a link to the resource
type IDLink struct {
	ID   string `bson:"id,omitempty"   json:"id"`
	Link string `bson:"link,omitempty" json:"link"`
}

// Event which has happened to an instance
type Event struct {
	Type          string `bson:"type,omitempty"           json:"type"`
	Time          string `bson:"time,omitempty"           json:"time"`
	Message       string `bson:"message,omitempty"        json:"message"`
	MessageOffset string `bson:"message_offset,omitempty" json:"message_offset"`
}

type InstanceResults struct {
	Items []Instance `json:"items"`
}

func (i *Instance) Defaults() error {
	if i.Job.ID == "" || i.Job.Link == "" {
		return errors.New("Missing job properties")
	}
	if i.State == "" {
		i.State = "created"
	}
	i.TotalObservations = new(int)
	i.InsertedObservations = new(int)
	i.Events = new([]Event)
	i.Headers = new([]string)

	return nil
}

func CreateInstance(reader io.Reader) (*Instance, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var instance Instance
	err = json.Unmarshal(bytes, &instance)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}
	return &instance, err
}
