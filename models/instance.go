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
	LastUpdated          time.Time     `bson:"last_updated,omitempty"                json:"last_updated,omitempty"`
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

// Dimension which is cached for the import process
type Dimension struct {
	Name        string    `bson:"name,omitempty"           json:"dimension_id"`
	Value       string    `bson:"value,omitempty"          json:"value"`
	NodeID      string    `bson:"node_id,omitempty"        json:"node_id"`
	InstanceID  string    `bson:"instance_id,omitempty"    json:"instance_id,omitempty"`
	LastUpdated time.Time `bson:"last_updated,omitempty"   json:"omitempty"`
}

// InstanceResults wraps instances objects for pagination
type InstanceResults struct {
	Items []Instance `json:"items"`
}

// DimensionNodeResults wraps dimension node objects for pagination
type DimensionNodeResults struct {
	Items []Dimension `json:"items"`
}

// DimensionValues holds all unique values for a dimension
type DimensionValues struct {
	Name   string   `json:"dimension_id"`
	Values []string `json:"values"`
}

// Validate the event structure
func (e *Event) Validate() error {
	if e.Message == "" || e.MessageOffset == "" || e.Time == nil || e.Type == "" {
		return errors.New("Missing properties")
	}
	return nil
}
