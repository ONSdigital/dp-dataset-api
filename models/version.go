package models

import "time"

// Version represents information related to a single version for an edition of a dataset
type Version struct {
	Alerts             *[]Alert             `bson:"alerts,omitempty"           json:"alerts,omitempty"`
	CollectionID       string               `bson:"collection_id,omitempty"    json:"collection_id,omitempty"`
	DatasetID          string               `bson:"-"                          json:"dataset_id,omitempty"`
	Dimensions         []Dimension          `bson:"dimensions,omitempty"       json:"dimensions,omitempty"`
	Downloads          *DownloadList        `bson:"downloads,omitempty"        json:"downloads,omitempty"`
	Edition            string               `bson:"edition,omitempty"          json:"edition,omitempty"`
	Headers            []string             `bson:"headers,omitempty"          json:"-"`
	ID                 string               `bson:"id,omitempty"               json:"id,omitempty"`
	LastUpdated        time.Time            `bson:"last_updated,omitempty"     json:"-"`
	LatestChanges      *[]LatestChange      `bson:"latest_changes,omitempty"   json:"latest_changes,omitempty"`
	Links              *VersionLinks        `bson:"links,omitempty"            json:"links,omitempty"`
	ReleaseDate        string               `bson:"release_date,omitempty"     json:"release_date,omitempty"`
	State              string               `bson:"state,omitempty"            json:"state,omitempty"`
	Temporal           *[]TemporalFrequency `bson:"temporal,omitempty"         json:"temporal,omitempty"`
	UsageNotes         *[]UsageNote         `bson:"usage_notes,omitempty"      json:"usage_notes,omitempty"`
	IsBasedOn          *IsBasedOn           `bson:"is_based_on,omitempty"      json:"is_based_on,omitempty"`
	Version            int                  `bson:"version,omitempty"          json:"version,omitempty"`
	Type               string               `bson:"type,omitempty"             json:"type,omitempty"`
	ETag               string               `bson:"e_tag"                      json:"-"`
	LowestGeography    string               `bson:"lowest_geography,omitempty" json:"lowest_geography,omitempty"`
	QualityDesignation QualityDesignation   `bson:"quality_designation,omitempty" json:"quality_designation,omitempty"`
	Distribution       *Distribution        `bson:"distribution,omitempty"     json:"distribution,omitempty"`
}

type Distribution struct {
	Title     string `bson:"title,omitempty" json:"title,omitempty"`
	MediaType string `bson:"media_type,omitempty" json:"media_type,omitempty"`
}
