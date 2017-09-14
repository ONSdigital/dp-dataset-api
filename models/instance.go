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

// CachedDimension is stored for the import process only. Each unique value for a dimension within a dataset is stored in the cache.
// A cached dimension is different to a dimension which is used by a client, the main difference is the client will have links to the
// code list API to get dimension values and meta data. This will return all possible values for a dataset, the cached dimensions could
// only be a subset. In the future it may contain information which can used, but the nodeID must be kept private.
type CachedDimension struct {
	Name        string     `bson:"name,omitempty"           json:"dimension_id"`
	Value       string     `bson:"value,omitempty"          json:"value"`
	NodeID      string     `bson:"node_id,omitempty"        json:"node_id"`
	InstanceID  string     `bson:"instance_id,omitempty"    json:"instance_id,omitempty"`
	LastUpdated time.Time `bson:"last_updated,omitempty"    json:"-"`
}

// InstanceResults wraps instances objects for pagination
type InstanceResults struct {
	Items []Instance `json:"items"`
}

// DimensionNodeResults wraps dimension node objects for pagination
type DimensionNodeResults struct {
	Items []CachedDimension `json:"items"`
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
