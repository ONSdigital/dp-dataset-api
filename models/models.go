package models

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
)

// DatasetList represents a structure for a list of datasets
type DatasetResults struct {
	Items []Dataset `json:"items"`
}

// Dataset represents information related to a single dataset
type Dataset struct {
	Contact     ContactDetails `bson:"contact,omitempty"        json:"contact,omitempty"`
	ID          string         `bson:"_id"                      json:"id"`
	NextRelease string         `bson:"next_release,omitempty"   json:"next_release,omitempty"`
	Name        string         `bson:"name,omitempty"           json:"name,omitempty"`
	EditionsURL string         `bson:"edition_url,omitempty"    json:"edition_url,omitempty"`
}

type Edition struct {
	ID          string `bson:"_id,omitempty"     json:"id,omitempty"`
	VersionsURL string `bson:"version_url"       json:"versions_url"`
}

type Version struct {
	ID           string `bson:"_id,omitempty"            json:"id,omitempty"`
	ReleaseDate  string `bson:"release_date,omitempty"   json:"release_date,omitempty"`
	DimensionURL string `bson:"dimension_url"            json:"dimension_url"`
}

// ContactDetails represents an object containing information of the contact
type ContactDetails struct {
	Email     string `bson:"email,omitempty"       json:"email,omitempty"`
	Name      string `bson:"name,omitempty"        json:"name,omitempty"`
	Telephone string `bson:"telephone,omitempty"   json:"telephone,omitempty"`
}

// CreateDataset manages the creation of a dataset from a reader
func CreateDataset(reader io.Reader) (*Dataset, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var datset Dataset
	err = json.Unmarshal(bytes, &datset)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &datset, nil
}
