package models

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"time"
)

// DatasetResults represents a structure for a list of datasets
type DatasetResults struct {
	Items []Dataset `json:"items"`
}

// Dataset represents information related to a single dataset
type Dataset struct {
	Contact     ContactDetails `bson:"contact,omitempty"        json:"contact,omitempty"`
	ID          string         `bson:"_id"                      json:"id"`
	NextRelease string         `bson:"next_release,omitempty"   json:"next_release,omitempty"`
	Name        string         `bson:"name,omitempty"           json:"name,omitempty"`
	Links       DatasetLinks   `bson:"links,omitempty"          json:"links,omitempty"`
	UpdatedAt   time.Time      `bson:"updated_at,omitempty"     json:"updated_at,omitempty"`
}

type DatasetLinks struct {
	Self     string `bson:"self,omitempty"        json:"self,omitempty"`
	Editions string `bson:"editions,omitempty"    json:"editions,omitempty"`
}

// ContactDetails represents an object containing information of the contact
type ContactDetails struct {
	Email     string `bson:"email,omitempty"      json:"email,omitempty"`
	Name      string `bson:"name,omitempty"       json:"name,omitempty"`
	Telephone string `bson:"telephone,omitempty"  json:"telephone,omitempty"`
}

type Edition struct {
	ID        string       `bson:"_id,omitempty"        json:"id,omitempty"`
	Name      string       `bson:"name,omitempty"       json:"name,omitempty"`
	Edition   string       `bson:"edition,omitempty"    json:"edition,omitempty"`
	Links     EditionLinks `bson:"links,omitempty"      json:"links,omitempty"`
	UpdatedAt time.Time    `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
}

type EditionLinks struct {
	Self     string `bson:"self,omitempty"        json:"self,omitempty"`
	Versions string `bson:"versions,omitempty"    json:"versions,omitempty"`
}

type Version struct {
	ID          string       `bson:"_id,omitempty"          json:"id,omitempty"`
	Name        string       `bson:"name,omitempty"         json:"name,omitempty"`
	Edition     string       `bson:"edition,omitempty"      json:"edition,omitempty"`
	Version     string       `bson:"version,omitempty"      json:"version,omitempty"`
	ReleaseDate string       `bson:"release_date,omitempty" json:"release_date,omitempty"`
	Links       VersionLinks `bson:"links,omitempty"        json:"links,omitempty"`
}

type VersionLinks struct {
	Self       string `bson:"self,omitempty"        json:"self,omitempty"`
	Dimensions string `bson:"dimensions,omitempty"  json:"dimensions,omitempty"`
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

// CreateEdition manages the creation of a edition from a reader
func CreateEdition(reader io.Reader) (*Edition, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var edition Edition
	err = json.Unmarshal(bytes, &edition)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &edition, nil
}

// CreateVersion manages the creation of a version from a reader
func CreateVersion(reader io.Reader) (*Version, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var version Version
	err = json.Unmarshal(bytes, &version)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &version, nil
}

// CreateContact manages the creation of a contact from a reader
func CreateContact(reader io.Reader) (*ContactDetails, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var contact ContactDetails
	err = json.Unmarshal(bytes, &contact)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &contact, nil
}
