package models

import (
	"errors"
	"fmt"
	"time"
)

// Instance which presents a single dataset being imported
type Instance struct {
	Alerts            *[]Alert             `bson:"alerts,omitempty"                      json:"alerts,omitempty"`
	InstanceID        string               `bson:"id,omitempty"                          json:"id,omitempty"`
	CollectionID      string               `bson:"collection_id,omitempty"               json:"collection_id,omitempty"`
	Dimensions        []CodeList           `bson:"dimensions,omitempty"                  json:"dimensions,omitempty"`
	Downloads         *DownloadList        `bson:"downloads,omitempty"                   json:"downloads,omitempty"`
	Edition           string               `bson:"edition,omitempty"                     json:"edition,omitempty"`
	Events            *[]Event             `bson:"events,omitempty"                      json:"events,omitempty"`
	Headers           *[]string            `bson:"headers,omitempty"                     json:"headers,omitempty"`
	LatestChanges     *[]LatestChange      `bson:"latest_changes,omitempty"              json:"latest_changes,omitempty"`
	Links             *InstanceLinks       `bson:"links,omitempty"                       json:"links,omitempty"`
	ReleaseDate       string               `bson:"release_date,omitempty"                json:"release_date,omitempty"`
	State             string               `bson:"state,omitempty"                       json:"state,omitempty"`
	Temporal          *[]TemporalFrequency `bson:"temporal,omitempty"                    json:"temporal,omitempty"`
	TotalObservations *int                 `bson:"total_observations,omitempty"          json:"total_observations,omitempty"`
	Version           int                  `bson:"version,omitempty"                     json:"version,omitempty"`
	LastUpdated       time.Time            `bson:"last_updated,omitempty"                json:"last_updated,omitempty"`
	ImportTasks       *InstanceImportTasks `bson:"import_tasks,omitempty"                json:"import_tasks"`
}

// InstanceImportTasks represents all of the tasks required to complete an import job.
type InstanceImportTasks struct {
	ImportObservations    *ImportObservationsTask `bson:"import_observations,omitempty"  json:"import_observations"`
	BuildHierarchyTasks   []*BuildHierarchyTask   `bson:"build_hierarchies,omitempty"    json:"build_hierarchies"`
	BuildSearchIndexTasks []*BuildSearchIndexTask `bson:"build_search_indexes,omitempty" json:"build_search_indexes"`
}

// ImportObservationsTask represents the task of importing instance observation data into the database.
type ImportObservationsTask struct {
	State                string `bson:"state,omitempty"             json:"state,omitempty"`
	InsertedObservations int64  `bson:"total_inserted_observations" json:"total_inserted_observations"`
}

// BuildHierarchyTask represents a task of importing a single hierarchy.
type BuildHierarchyTask struct {
	State         string `bson:"state,omitempty"          json:"state,omitempty"`
	DimensionName string `bson:"dimension_name,omitempty" json:"dimension_name,omitempty"`
	CodeListID    string `bson:"code_list_id,omitempty"   json:"code_list_id,omitempty"`
}

// BuildSearchIndexTask represents a task of importing a single search index into search.
type BuildSearchIndexTask struct {
	State         string `bson:"state,omitempty"          json:"state,omitempty"`
	DimensionName string `bson:"dimension_name,omitempty" json:"dimension_name,omitempty"`
}

// CodeList for a dimension within an instance
type CodeList struct {
	Description string `json:"description"`
	HRef        string `json:"href"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Label       string `json:"label"`
}

// InstanceLinks holds all links for an instance
type InstanceLinks struct {
	Job        *IDLink `bson:"job,omitempty"        json:"job"`
	Dataset    *IDLink `bson:"dataset,omitempty"    json:"dataset,omitempty"`
	Dimensions *IDLink `bson:"dimensions,omitempty" json:"dimensions,omitempty"`
	Edition    *IDLink `bson:"edition,omitempty"    json:"edition,omitempty"`
	Version    *IDLink `bson:"version,omitempty"    json:"version,omitempty"`
	Self       *IDLink `bson:"self,omitempty"       json:"self,omitempty"`
	Spatial    *IDLink `bson:"spatial,omitempty"    json:"spatial,omitempty"`
}

// IDLink holds the id and a link to the resource
type IDLink struct {
	ID   string `bson:"id,omitempty"   json:"id,omitempty"`
	HRef string `bson:"href,omitempty" json:"href,omitempty"`
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
		return errors.New("missing properties")
	}
	return nil
}

var validStates = map[string]int{
	CreatedState:          1,
	SubmittedState:        1,
	CompletedState:        1,
	EditionConfirmedState: 1,
	AssociatedState:       1,
	PublishedState:        1,
}

// ValidateStateFilter checks the list of filter states from a whitelist
func ValidateStateFilter(filterList []string) error {
	var invalidFilterStateValues []string

	for _, filter := range filterList {
		if _, ok := validStates[filter]; !ok {
			invalidFilterStateValues = append(invalidFilterStateValues, filter)
		}
	}

	if invalidFilterStateValues != nil {
		err := fmt.Errorf("bad request - invalid filter state values: %v", invalidFilterStateValues)
		return err
	}

	return nil
}

// ValidateInstanceState checks the list of instance states from a whitelist
func ValidateInstanceState(state string) error {
	var invalidInstantStateValues []string

	if _, ok := validStates[state]; !ok {
		invalidInstantStateValues = append(invalidInstantStateValues, state)
	}

	if invalidInstantStateValues != nil {
		err := fmt.Errorf("bad request - invalid filter state values: %v", invalidInstantStateValues)
		return err
	}

	return nil
}
