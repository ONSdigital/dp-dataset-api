package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	uuid "github.com/satori/go.uuid"
)

const created = "created"

// DatasetResults represents a structure for a list of datasets
type DatasetResults struct {
	Items []*Dataset `json:"items"`
}

// EditionResults represents a structure for a list of editions for a dataset
type EditionResults struct {
	Items []Edition `json:"items"`
}

// VersionResults represents a structure for a list of versions for an edition of a dataset
type VersionResults struct {
	Items []Version `json:"items"`
}

// DatasetUpdate represents an evolving dataset with the current dataset and the updated dataset
type DatasetUpdate struct {
	ID      string   `bson:"_id,omitempty"         json:"id,omitempty"`
	Current *Dataset `bson:"current,omitempty"     json:"current,omitempty"`
	Next    *Dataset `bson:"next,omitempty"        json:"next,omitempty"`
}

// Dataset represents information related to a single dataset
type Dataset struct {
	CollectionID      string           `bson:"collection_id,omitempty"          json:"collection_id,omitempty"`
	Contacts          []ContactDetails `bson:"contacts,omitempty"               json:"contacts,omitempty"`
	Description       string           `bson:"description,omitempty"            json:"description,omitempty"`
	Keywords          []string         `bson:"keywords,omitempty"               json:"keywords,omitempty"`
	ID                string           `bson:"_id,omitempty"                    json:"id,omitempty"`
	Links             DatasetLinks     `bson:"links,omitempty"                  json:"links,omitempty"`
	Methodologies     []GeneralDetails `bson:"methodologies,omitempty"          json:"methodologies,omitempty"`
	NationalStatistic bool             `bson:"national_statistic,omitempty"     json:"national_statistic,omitempty"`
	NextRelease       string           `bson:"next_release,omitempty"           json:"next_release,omitempty"`
	Publications      []GeneralDetails `bson:"publications,omitempty"           json:"publications,omitempty"`
	Publisher         Publisher        `bson:"publisher,omitempty"              json:"publisher,omitempty"`
	QMI               GeneralDetails   `bson:"qmi,omitempty"                    json:"qmi,omitempty"`
	RelatedDatasets   []GeneralDetails `bson:"related_datasets,omitempty"       json:"related_datasets,omitempty"`
	ReleaseFrequency  string           `bson:"release_frequency,omitempty"      json:"release_frequency,omitempty"`
	State             string           `bson:"state,omitempty"                  json:"state,omitempty"`
	Theme             string           `bson:"theme,omitempty"                  json:"theme,omitempty"`
	Title             string           `bson:"title,omitempty"                  json:"title,omitempty"`
	URI               string           `bson:"uri,omitempty"                    json:"uri,omitempty"`
	LastUpdated       time.Time        `bson:"last_updated,omitempty"           json:"-"`
}

// DatasetLinks represents a list of specific links related to the dataset resource
type DatasetLinks struct {
	Editions      LinkObject `bson:"editions,omitempty"        json:"editions,omitempty"`
	LatestVersion LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty"`
	Self          LinkObject `bson:"self,omitempty"            json:"self,omitempty"`
}

// LinkObject represents a generic structure for all links
type LinkObject struct {
	ID   string `bson:"id,omitempty"    json:"id,omitempty"`
	HRef string `bson:"href,omitempty"  json:"href,omitempty"`
}

// GeneralDetails represents generic fields stored against an object (reused)
type GeneralDetails struct {
	Description string `bson:"description,omitempty"    json:"description,omitempty"`
	HRef        string `bson:"href,omitempty"           json:"href,omitempty"`
	Title       string `bson:"title,omitempty"          json:"title,omitempty"`
}

type Contact struct {
	ID          string    `bson:"_id,omitempty"            json:"id,omitempty"`
	Email       string    `bson:"email,omitempty"          json:"email,omitempty"`
	LastUpdated time.Time `bson:"last_updated,omitempty"   json:"-"`
	Name        string    `bson:"name,omitempty"           json:"name,omitempty"`
	Telephone   string    `bson:"telephone,omitempty"      json:"telephone,omitempty"`
}

// ContactDetails represents an object containing information of the contact
type ContactDetails struct {
	Email     string `bson:"email,omitempty"      json:"email,omitempty"`
	Name      string `bson:"name,omitempty"       json:"name,omitempty"`
	Telephone string `bson:"telephone,omitempty"  json:"telephone,omitempty"`
}

// Edition represents information related to a single edition for a dataset
type Edition struct {
	Edition     string       `bson:"edition,omitempty"      json:"edition,omitempty"`
	ID          string       `bson:"_id,omitempty"          json:"id,omitempty"`
	Links       EditionLinks `bson:"links,omitempty"        json:"links,omitempty"`
	State       string       `bson:"state,omitempty"        json:"state,omitempty"`
	LastUpdated time.Time    `bson:"last_updated,omitempty" json:"-"`
}

// EditionLinks represents a list of specific links related to the edition resource of a dataset
type EditionLinks struct {
	Dataset  LinkObject `bson:"dataset,omitempty"     json:"dataset,omitempty"`
	Self     LinkObject `bson:"self,omitempty"        json:"self,omitempty"`
	Versions LinkObject `bson:"versions,omitempty"    json:"versions,omitempty"`
}

// Publisher represents an object containing information of the publisher
type Publisher struct {
	Name string `bson:"name,omitempty" json:"name,omitempty"`
	Type string `bson:"type,omitempty" json:"type,omitempty"`
	HRef string `bson:"href,omitempty" json:"href,omitempty"`
}

// Version represents information related to a single version for an edition of a dataset
type Version struct {
	CollectionID string       `bson:"collection_id,omitempty" json:"collection_id,omitempty"`
	Downloads    Downloads    `bson:"downloads,omitempty"     json:"downloads,omitempty"`
	Edition      string       `bson:"edition,omitempty"       json:"edition,omitempty"`
	ID           string       `bson:"_id,omitempty"           json:"id,omitempty"`
	InstanceID   string       `bson:"instance_id,omitempty"   json:"instance_id,omitempty"`
	License      string       `bson:"license,omitempty"       json:"license,omitempty"`
	Links        VersionLinks `bson:"links,omitempty"         json:"links,omitempty"`
	ReleaseDate  string       `bson:"release_date,omitempty"  json:"release_date,omitempty"`
	State        string       `bson:"state,omitempty"         json:"state,omitempty"`
	LastUpdated  time.Time    `bson:"last_updated,omitempty"  json:"-"`
	Version      int          `bson:"version,omitempty"       json:"version,omitempty"`
}

// Downloads represents a list of objects of containing information on the downloadable files
type Downloads struct {
	CSV DownloadObject `bson:"csv,omitempty" json:"csv,omitempty"`
	XLS DownloadObject `bson:"xls,omitempty" json:"xls,omitempty"`
}

// DownloadObject represents information on the downloadable file
type DownloadObject struct {
	URL  string `bson:"url,omitempty"  json:"url,omitempty"`
	Size string `bson:"size,omitempty" json:"size,omitempty"`
}

// VersionLinks represents a list of specific links related to the version resource for an edition of a dataset
type VersionLinks struct {
	Dataset    LinkObject `bson:"dataset,omitempty"     json:"dataset,omitempty"`
	Dimensions LinkObject `bson:"dimensions,omitempty"  json:"dimensions,omitempty"`
	Edition    LinkObject `bson:"edition,omitempty"     json:"edition,omitempty"`
	Self       LinkObject `bson:"self,omitempty"        json:"self,omitempty"`
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

	err = json.Unmarshal(bytes, &dataset)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	// Overwrite state to created
	dataset.State = created

	return &dataset, nil
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
	version.State = created

	err = json.Unmarshal(bytes, &version)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &version, nil
}

// CreateContact manages the creation of a contact from a reader
func CreateContact(reader io.Reader) (*Contact, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var contact Contact
	err = json.Unmarshal(bytes, &contact)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	// Create unique id
	contact.ID = (uuid.NewV4()).String()

	return &contact, nil
}

// ValidateVersion checks the content of the version structure
func ValidateVersion(version *Version) error {
	var hasAssociation bool

	switch version.State {
	case "created":
	case "associated":
		hasAssociation = true
	case "published":
		hasAssociation = true
	default:
		return errors.New("Incorrect state, can be one of the following: created, associated or published")
	}

	if hasAssociation && version.CollectionID == "" {
		return errors.New("Missing collection_id for association between version and a collection")
	}

	var missingFields []string

	if version.InstanceID == "" {
		missingFields = append(missingFields, "instance_id")
	}

	if version.License == "" {
		missingFields = append(missingFields, "license")
	}

	if version.ReleaseDate == "" {
		missingFields = append(missingFields, "release_date")
	}

	if missingFields != nil {
		return fmt.Errorf("Missing mandatory fields: %v", missingFields)
	}

	return nil
}
