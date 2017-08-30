package models

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"time"

	uuid "github.com/satori/go.uuid"
)

const unpublished = "unpublished"

// DatasetResults represents a structure for a list of datasets
type DatasetResults struct {
	Items []Dataset `json:"items"`
}

type EditionResults struct {
	Items []Edition `json:"items"`
}

type VersionResults struct {
	Items []Version `json:"items"`
}

// Dataset represents information related to a single dataset
type Dataset struct {
	Contact     ContactDetails `bson:"contact,omitempty"        json:"contact,omitempty"`
	DatasetID   string         `bson:"dataset_id"               json:"dataset_id"`
	Description string         `bson:"description"              json:"description"`
	ID          string         `bson:"_id"                      json:"id"`
	Links       DatasetLinks   `bson:"links,omitempty"          json:"links,omitempty"`
	NextRelease string         `bson:"next_release,omitempty"   json:"next_release,omitempty"`
	Periodicity string         `bson:"periodicity"              json:"periodicity"`
	Publisher   Publisher      `bson:"publisher,omitempty"      json:"publisher,omitempty"`
	State       string         `bson:"state,omitempty"          json:"state,omitempty"`
	Theme       string         `bson:"theme,omitempty"          json:"thems,omitempty"`
	Title       string         `bson:"title,omitempty"          json:"title,omitempty"`
	UpdatedAt   time.Time      `bson:"updated_at,omitempty"     json:"updated_at,omitempty"`
}

type DatasetLinks struct {
	Editions      string `bson:"editions,omitempty"        json:"editions,omitempty"`
	LatestVersion string `bson:"latest_version,omitempty"  json:"latest_version,omitempty"`
	Self          string `bson:"self,omitempty"            json:"self,omitempty"`
}

// ContactDetails represents an object containing information of the contact
type ContactDetails struct {
	Email     string `bson:"email,omitempty"      json:"email,omitempty"`
	Name      string `bson:"name,omitempty"       json:"name,omitempty"`
	Telephone string `bson:"telephone,omitempty"  json:"telephone,omitempty"`
}

type Edition struct {
	Edition   string       `bson:"edition,omitempty"    json:"edition,omitempty"`
	ID        string       `bson:"_id,omitempty"        json:"id,omitempty"`
	Links     EditionLinks `bson:"links,omitempty"      json:"links,omitempty"`
	State     string       `bson:"state,omitempty"      json:"state,omitempty"`
	UpdatedAt time.Time    `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
}

type EditionLinks struct {
	Dataset  string `bson:"dataset,omitempty"     json:"dataset,omitempty"`
	Self     string `bson:"self,omitempty"        json:"self,omitempty"`
	Versions string `bson:"versions,omitempty"    json:"versions,omitempty"`
}

type Publisher struct {
	Name string `bson:"name,omitempty" json:"name,omitempty"`
	Type string `bson:"type,omitempty" json:"type,omitempty"`
	URL  string `bson:"url,omitempty"  json:"url,omitempty"`
}

type Version struct {
	Edition     string       `bson:"edition,omitempty"      json:"edition,omitempty"`
	ID          string       `bson:"_id,omitempty"          json:"id,omitempty"`
	License     string       `bson:"license,omitempty"      json:"license,omitempty"`
	Links       VersionLinks `bson:"links,omitempty"        json:"links,omitempty"`
	ReleaseDate string       `bson:"release_date,omitempty" json:"release_date,omitempty"`
	State       string       `bson:"state,omitempty"        json:"state,omitempty"`
	UpdatedAt   time.Time    `bson:"updated_at,omitempty"   json:"updated_at,omitempty"`
	Version     string       `bson:"version,omitempty"      json:"version,omitempty"`
}

type VersionLinks struct {
	Dataset    string `bson:"dataset,omitempty"     json:"dataset,omitempty"`
	Dimensions string `bson:"dimensions,omitempty"  json:"dimensions,omitempty"`
	Edition    string `bson:"edition,omitempty"     json:"edition,omitempty"`
	Self       string `bson:"self,omitempty"        json:"self,omitempty"`
}

// CreateDataset manages the creation of a dataset from a reader
func CreateDataset(reader io.Reader) (*Dataset, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var dataset Dataset
	// Create unique id
	dataset.ID = uuid.NewV4().String()
	// set default state to be unpublished
	dataset.State = unpublished

	err = json.Unmarshal(bytes, &dataset)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &dataset, nil
}

// CreateEdition manages the creation of a edition from a reader
func CreateEdition(reader io.Reader) (*Edition, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var edition Edition
	// Create unique id
	edition.ID = (uuid.NewV4()).String()
	// set default state to be unpublished
	edition.State = unpublished

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
	// Create unique id
	version.ID = (uuid.NewV4()).String()
	// set default state to be unpublished
	version.State = unpublished

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
