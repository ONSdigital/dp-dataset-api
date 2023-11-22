package models

import (
	//nolint:gosec //not used for secure purposes
	"crypto/sha1"
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"go.mongodb.org/mongo-driver/bson"
	bsonprim "go.mongodb.org/mongo-driver/bson/primitive"
)

type InstanceList struct {
	Page       `groups:"instances"`
	Links      *PageLinks    `json:"_links" groups:"instances"`
	Items      []*LDInstance `json:"items" groups:"instances"`
	LinkedData `groups:"all"`
}

type LDInstance struct {
	Events      *[]Event         `bson:"events,omitempty"                      json:"events,omitempty" groups:"instances,instance"`
	InstanceID  string           `bson:"identifier,omitempty"                          json:"instance_id,omitempty" groups:"instances,instance"`
	Links       *LDInstanceLinks `bson:"_links,omitempty" json:"_links,omitempty" groups:"instances,instance"`
	LastUpdated time.Time        `bson:"last_updated,omitempty" json:"last_updated,omitempty" groups:"instances,instance"` //this is stored on instances but causes a collision if marshalled to JSON
	LDEdition   `bson:",inline"`

	// Collection and State could move into this struct if not needed publicly
}

// Instance which presents a single dataset being imported
type Instance struct {
	Alerts            *[]Alert             `bson:"alerts,omitempty"                      json:"alerts,omitempty"`
	CollectionID      string               `bson:"collection_id,omitempty"               json:"collection_id,omitempty"`
	Dimensions        []Dimension          `bson:"dimensions,omitempty"                  json:"dimensions,omitempty"`
	Downloads         *DownloadList        `bson:"downloads,omitempty"                   json:"downloads,omitempty"`
	Edition           string               `bson:"edition,omitempty"                     json:"edition,omitempty"`
	Events            *[]Event             `bson:"events,omitempty"                      json:"events,omitempty"`
	Headers           *[]string            `bson:"headers,omitempty"                     json:"headers,omitempty"`
	InstanceID        string               `bson:"id,omitempty"                          json:"id,omitempty"`
	LastUpdated       time.Time            `bson:"last_updated,omitempty"                json:"last_updated,omitempty"`
	ETag              string               `bson:"e_tag"                                 json:"-"`
	LatestChanges     *[]LatestChange      `bson:"latest_changes,omitempty"              json:"latest_changes,omitempty"`
	Links             *InstanceLinks       `bson:"links,omitempty"                       json:"links,omitempty"`
	ReleaseDate       string               `bson:"release_date,omitempty"                json:"release_date,omitempty"`
	State             string               `bson:"state,omitempty"                       json:"state,omitempty"`
	Temporal          *[]TemporalFrequency `bson:"temporal,omitempty"                    json:"temporal,omitempty"`
	TotalObservations *int                 `bson:"total_observations,omitempty"          json:"total_observations,omitempty"`
	UniqueTimestamp   bsonprim.Timestamp   `bson:"unique_timestamp"                      json:"-"`
	Version           int                  `bson:"version,omitempty"                     json:"version,omitempty"`
	Type              string               `bson:"type,omitempty"                        json:"type,omitempty"`
	IsBasedOn         *IsBasedOn           `bson:"is_based_on,omitempty"                 json:"is_based_on,omitempty"`
	LowestGeography   string               `bson:"lowest_geography,omitempty"            json:"lowest_geography,omitempty"`
}

// Hash generates a SHA-1 hash of the instance struct. SHA-1 is not cryptographically safe,
// but it has been selected for performance as we are only interested in uniqueness.
// ETag field value is ignored when generating a hash.
// An optional byte array can be provided to append to the hash.
// This can be used, for example, to calculate a hash of this instance and an update applied to it.
func (i *Instance) Hash(extraBytes []byte) (string, error) {
	//nolint:gosec // not being used for secure purposes
	h := sha1.New()

	// copy by value to ignore ETag without affecting i
	i2 := *i
	i2.ETag = ""

	instanceBytes, err := bson.Marshal(i2)
	if err != nil {
		return "", err
	}

	if _, err := h.Write(append(instanceBytes, extraBytes...)); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// InstanceLinks holds all links for an instance
type InstanceLinks struct {
	Dataset    *LinkObject `bson:"dataset,omitempty"    json:"dataset,omitempty"`
	Dimensions *LinkObject `bson:"dimensions,omitempty" json:"dimensions,omitempty"`
	Edition    *LinkObject `bson:"edition,omitempty"    json:"edition,omitempty"`
	Job        *LinkObject `bson:"job,omitempty"        json:"job"`
	Self       *LinkObject `bson:"self,omitempty"       json:"self,omitempty"`
	Spatial    *LinkObject `bson:"spatial,omitempty"    json:"spatial,omitempty"`
	Version    *LinkObject `bson:"version,omitempty"    json:"version,omitempty"`
}

// Event which has happened to an instance
type Event struct {
	Message       string     `bson:"message,omitempty"        json:"message"`
	MessageOffset string     `bson:"message_offset,omitempty" json:"message_offset"`
	Time          *time.Time `bson:"time,omitempty"           json:"time"`
	Type          string     `bson:"type,omitempty"           json:"type"`
}

// Validate the event structure
func (e *Event) Validate() error {
	if e.Message == "" || e.MessageOffset == "" || e.Time == nil || e.Type == "" {
		return errs.ErrMissingParameters
	}
	return nil
}
