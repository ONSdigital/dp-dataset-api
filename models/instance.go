package models

import (
	"errors"
	"time"
)

// Instance which presents a single dataset being imported
type Instance struct {
	InstanceID           string        `bson:"id,omitempty"                          json:"id,omitempty"`
	Links                InstanceLinks `bson:"links,omitempty"                       json:"links,omitempty"`
	State                string        `bson:"state,omitempty"                       json:"state,omitempty"`
	Events               *[]Event      `bson:"events,omitempty"                      json:"events,omitempty"`
	TotalObservations    *int          `bson:"total_observations,omitempty"          json:"total_observations,omitempty"`
	InsertedObservations *int          `bson:"total_inserted_observations,omitempty" json:"total_inserted_observations,omitempty"`
	Headers              *[]string     `bson:"headers,omitempty"                     json:"headers,omitempty"`
	Dimensions           []CodeList    `bson:"dimensions,omitempty"                  json:"dimensions,omitempty"`
	LastUpdated          time.Time     `bson:"last_updated,omitempty"                json:"last_updated,omitempty"`
}

// CodeList for a dimension within an instance
type CodeList struct {
	ID   string `json:"id"`
	HRef string `json:"href"`
	Name string `json:"name"`
}

// InstanceLinks holds all links for an instance
type InstanceLinks struct {
	Job IDLink `bson:"job,omitempty"   json:"job"`
}

// IDLink holds the id and a link to the resource
type IDLink struct {
	ID   string `bson:"id,omitempty"   json:"id"`
	HRef string `bson:"href,omitempty" json:"href"`
}

// Event which has happened to an instance
type Event struct {
	Type          string     `bson:"type,omitempty"           json:"type"`
	Time          *time.Time `bson:"time,omitempty"           json:"time"`
	Message       string     `bson:"message,omitempty"        json:"message"`
	MessageOffset string     `bson:"message_offset,omitempty" json:"message_offset"`
}

// InstanceResults wraps instances objects for pagination
type InstanceResults struct {
	Items []Instance `json:"items"`
}

// Validate the event structure
func (e *Event) Validate() error {
	if e.Message == "" || e.MessageOffset == "" || e.Time == nil || e.Type == "" {
		return errors.New("Missing properties")
	}
	return nil
}
