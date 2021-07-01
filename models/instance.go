package models

import (
	"fmt"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/globalsign/mgo/bson"
)

// Instance which presents a single dataset being imported
type Instance struct {
	Alerts            *[]Alert             `bson:"alerts,omitempty"                      json:"alerts,omitempty"`
	CollectionID      string               `bson:"collection_id,omitempty"               json:"collection_id,omitempty"`
	Dimensions        []Dimension          `bson:"dimensions,omitempty"                  json:"dimensions,omitempty"`
	Downloads         *DownloadList        `bson:"downloads,omitempty"                   json:"downloads,omitempty"`
	Edition           string               `bson:"edition,omitempty"                     json:"edition,omitempty"`
	Events            *[]Event             `bson:"events,omitempty"                      json:"events,omitempty"`
	Headers           *[]string            `bson:"headers,omitempty"                     json:"headers,omitempty"`
	ImportTasks       *InstanceImportTasks `bson:"import_tasks,omitempty"                json:"import_tasks"`
	InstanceID        string               `bson:"id,omitempty"                          json:"id,omitempty"`
	LastUpdated       time.Time            `bson:"last_updated,omitempty"                json:"last_updated,omitempty"`
	LatestChanges     *[]LatestChange      `bson:"latest_changes,omitempty"              json:"latest_changes,omitempty"`
	Links             *InstanceLinks       `bson:"links,omitempty"                       json:"links,omitempty"`
	ReleaseDate       string               `bson:"release_date,omitempty"                json:"release_date,omitempty"`
	State             string               `bson:"state,omitempty"                       json:"state,omitempty"`
	Temporal          *[]TemporalFrequency `bson:"temporal,omitempty"                    json:"temporal,omitempty"`
	TotalObservations *int                 `bson:"total_observations,omitempty"          json:"total_observations,omitempty"`
	UniqueTimestamp   bson.MongoTimestamp  `bson:"unique_timestamp"                      json:"-"`
	Version           int                  `bson:"version,omitempty"                     json:"version,omitempty"`
	Type              string               `bson:"type,omitempty"                        json:"type,omitempty"`
	IsBasedOn         *IsBasedOn           `bson:"is_based_on,omitempty"                 json:"is_based_on,omitempty"`
}

// InstanceImportTasks represents all of the tasks required to complete an import job.
type InstanceImportTasks struct {
	BuildHierarchyTasks   []*BuildHierarchyTask   `bson:"build_hierarchies,omitempty"    json:"build_hierarchies"`
	BuildSearchIndexTasks []*BuildSearchIndexTask `bson:"build_search_indexes,omitempty" json:"build_search_indexes"`
	ImportObservations    *ImportObservationsTask `bson:"import_observations,omitempty"  json:"import_observations"`
}

// ImportObservationsTask represents the task of importing instance observation data into the database.
type ImportObservationsTask struct {
	InsertedObservations int64  `bson:"total_inserted_observations" json:"total_inserted_observations"`
	State                string `bson:"state,omitempty"             json:"state,omitempty"`
}

// BuildHierarchyTask represents a task of importing a single hierarchy.
type BuildHierarchyTask struct {
	DimensionID        string `bson:"code_list_id,omitempty"   json:"code_list_id,omitempty"`
	GenericTaskDetails `bson:",inline"`
}

type GenericTaskDetails struct {
	DimensionName string `bson:"dimension_name,omitempty" json:"dimension_name,omitempty"`
	State         string `bson:"state,omitempty"          json:"state,omitempty"`
}

// BuildSearchIndexTask represents a task of importing a single search index into search.
type BuildSearchIndexTask struct {
	GenericTaskDetails `bson:",inline"`
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

// ValidateImportTask checks the task contains mandatory fields
func ValidateImportTask(task GenericTaskDetails) error {
	var missingFields []string

	if task.DimensionName == "" {
		missingFields = append(missingFields, "dimension_name")
	}

	if task.State == "" {
		missingFields = append(missingFields, "state")
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("bad request - missing mandatory fields: %v", missingFields)
	}

	if task.State != CompletedState {
		return fmt.Errorf("bad request - invalid task state value: %v", task.State)
	}

	return nil
}
