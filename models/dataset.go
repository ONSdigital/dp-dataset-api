package models

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

// DatasetResults represents a structure for a list of datasets
type DatasetResults struct {
	Items []*Dataset `json:"items"`
}

// DatasetUpdateResults represents a structure for a list of evolving dataset
// with the current dataset and the updated dataset
type DatasetUpdateResults struct {
	Items []DatasetUpdate `json:"items"`
}

// EditionResults represents a structure for a list of editions for a dataset
type EditionResults struct {
	Items []*Edition `json:"items"`
}

// EditionUpdateResults represents a structure for a list of evolving dataset
// with the current dataset and the updated dataset
type EditionUpdateResults struct {
	Items []*EditionUpdate `json:"items"`
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
	License           string           `bson:"license,omitempty"                json:"license,omitempty"`
	Links             *DatasetLinks    `bson:"links,omitempty"                  json:"links,omitempty"`
	Methodologies     []GeneralDetails `bson:"methodologies,omitempty"          json:"methodologies,omitempty"`
	NationalStatistic *bool            `bson:"national_statistic,omitempty"     json:"national_statistic,omitempty"`
	NextRelease       string           `bson:"next_release,omitempty"           json:"next_release,omitempty"`
	Publications      []GeneralDetails `bson:"publications,omitempty"           json:"publications,omitempty"`
	Publisher         *Publisher       `bson:"publisher,omitempty"              json:"publisher,omitempty"`
	QMI               *GeneralDetails  `bson:"qmi,omitempty"                    json:"qmi,omitempty"`
	RelatedDatasets   []GeneralDetails `bson:"related_datasets,omitempty"       json:"related_datasets,omitempty"`
	ReleaseFrequency  string           `bson:"release_frequency,omitempty"      json:"release_frequency,omitempty"`
	State             string           `bson:"state,omitempty"                  json:"state,omitempty"`
	Theme             string           `bson:"theme,omitempty"                  json:"theme,omitempty"`
	Title             string           `bson:"title,omitempty"                  json:"title,omitempty"`
	UnitOfMeasure     string           `bson:"unit_of_measure,omitempty"        json:"unit_of_measure,omitempty"`
	URI               string           `bson:"uri,omitempty"                    json:"uri,omitempty"`
	LastUpdated       time.Time        `bson:"last_updated,omitempty"           json:"-"`
}

// DatasetLinks represents a list of specific links related to the dataset resource
type DatasetLinks struct {
	AccessRights  *LinkObject `bson:"access_rights,omitempty"   json:"access_rights,omitempty"`
	Editions      *LinkObject `bson:"editions,omitempty"        json:"editions,omitempty"`
	LatestVersion *LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty"`
	Self          *LinkObject `bson:"self,omitempty"            json:"self,omitempty"`
	Taxonomy      *LinkObject `bson:"taxonomy,omitempty"        json:"taxonomy,omitempty"`
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

// Contact represents information of individual contact details
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

// EditionUpdate represents an evolving edition containing both the next and current edition
type EditionUpdate struct {
	ID      string   `bson:"id,omitempty"         json:"id,omitempty"`
	Current *Edition `bson:"current,omitempty"     json:"current,omitempty"`
	Next    *Edition `bson:"next,omitempty"        json:"next,omitempty"`
}

// EditionUpdateLinks represents those links common the both the current and next edition
type EditionUpdateLinks struct {
	Dataset       *LinkObject `bson:"dataset,omitempty"        json:"dataset,omitempty"`
	Self          *LinkObject `bson:"self,omitempty"           json:"self,omitempty"`
	Versions      *LinkObject `bson:"versions,omitempty"       json:"versions,omitempty"`
	LatestVersion *LinkObject `bson:"latest_version,omitempty" json:"latest_version,omitempty"`
}

// Edition represents information related to a single edition for a dataset
type Edition struct {
	Edition     string              `bson:"edition,omitempty"     json:"edition,omitempty"`
	ID          string              `bson:"id,omitempty"          json:"id,omitempty"`
	Links       *EditionUpdateLinks `bson:"links,omitempty"       json:"links,omitempty"`
	State       string              `bson:"state,omitempty"        json:"state,omitempty"`
	LastUpdated time.Time           `bson:"last_updated,omitempty" json:"-"`
}

// Publisher represents an object containing information of the publisher
type Publisher struct {
	Name string `bson:"name,omitempty" json:"name,omitempty"`
	Type string `bson:"type,omitempty" json:"type,omitempty"`
	HRef string `bson:"href,omitempty" json:"href,omitempty"`
}

// Version represents information related to a single version for an edition of a dataset
type Version struct {
	Alerts        *[]Alert             `bson:"alerts,omitempty"         json:"alerts,omitempty"`
	CollectionID  string               `bson:"collection_id,omitempty"  json:"collection_id,omitempty"`
	Dimensions    []CodeList           `bson:"dimensions,omitempty"     json:"dimensions,omitempty"`
	Downloads     *DownloadList        `bson:"downloads,omitempty"      json:"downloads,omitempty"`
	Edition       string               `bson:"edition,omitempty"        json:"edition,omitempty"`
	ID            string               `bson:"id,omitempty"             json:"id,omitempty"`
	LatestChanges *[]LatestChange      `bson:"latest_changes,omitempty" json:"latest_changes,omitempty"`
	Links         *VersionLinks        `bson:"links,omitempty"          json:"links,omitempty"`
	ReleaseDate   string               `bson:"release_date,omitempty"   json:"release_date,omitempty"`
	State         string               `bson:"state,omitempty"          json:"state,omitempty"`
	Temporal      *[]TemporalFrequency `bson:"temporal,omitempty"       json:"temporal,omitempty"`
	LastUpdated   time.Time            `bson:"last_updated,omitempty"   json:"-"`
	Version       int                  `bson:"version,omitempty"        json:"version,omitempty"`
	UsageNotes    *[]UsageNote         `bson:"usage_notes,omitempty"     json:"usage_notes,omitempty"`
}

// Alert represents an object containing information on an alert
type Alert struct {
	Date        string `bson:"date,omitempty"        json:"date,omitempty"`
	Description string `bson:"description,omitempty" json:"description,omitempty"`
	Type        string `bson:"type,omitempty"        json:"type,omitempty"`
}

// DownloadList represents a list of objects of containing information on the downloadable files
type DownloadList struct {
	CSV *DownloadObject `bson:"csv,omitempty" json:"csv,omitempty"`
	XLS *DownloadObject `bson:"xls,omitempty" json:"xls,omitempty"`
}

// DownloadObject represents information on the downloadable file
type DownloadObject struct {
	URL string `bson:"url,omitempty"  json:"url,omitempty"`
	// TODO size is in b and probably should be an int64 instead of a string this
	// will have to change for several services (filter API, exporter services and web)
	Size    string `bson:"size,omitempty" json:"size,omitempty"`
	Public  string `bson:"public,omitempty" json:"public,omitempty"`
	Private string `bson:"private,omitempty" json:"private,omitempty"`
}

// LatestChange represents an object contining
// information on a single change between versions
type LatestChange struct {
	Description string `bson:"description,omitempty" json:"description,omitempty"`
	Name        string `bson:"name,omitempty"        json:"name,omitempty"`
	Type        string `bson:"type,omitempty"        json:"type,omitempty"`
}

// TemporalFrequency represents a frequency for a particular period of time
type TemporalFrequency struct {
	EndDate   string `bson:"end_date,omitempty"    json:"end_date,omitempty"`
	Frequency string `bson:"frequency,omitempty"   json:"frequency,omitempty"`
	StartDate string `bson:"start_date,omitempty"  json:"start_date,omitempty"`
}

type UsageNote struct {
	Title string `bson:"title,omitempty"    json:"title,omitempty"`
	Note  string `bson:"note,omitempty"     json:"note,omitempty"`
}

// VersionLinks represents a list of specific links related to the version resource for an edition of a dataset
type VersionLinks struct {
	Dataset    *LinkObject `bson:"dataset,omitempty"     json:"dataset,omitempty"`
	Dimensions *LinkObject `bson:"dimensions,omitempty"  json:"dimensions,omitempty"`
	Edition    *LinkObject `bson:"edition,omitempty"     json:"edition,omitempty"`
	Self       *LinkObject `bson:"self,omitempty"        json:"self,omitempty"`
	Spatial    *LinkObject `bson:"spatial,omitempty"     json:"spatial,omitempty"`
	Version    *LinkObject `bson:"version,omitempty"     json:"-"`
}

var validVersionStates = map[string]int{
	EditionConfirmedState: 1,
	AssociatedState:       1,
	PublishedState:        1,
}

// CheckState checks state against a whitelist of valid states
func CheckState(docType, state string) error {
	var states map[string]int
	switch docType {
	case "version":
		states = validVersionStates
	default:
		states = validStates
	}

	for key := range states {
		if state == key {
			return nil
		}
	}

	return errs.ErrResourceState
}

// CreateDataset manages the creation of a dataset from a reader
func CreateDataset(reader io.Reader) (*Dataset, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var dataset Dataset

	err = json.Unmarshal(b, &dataset)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}
	return &dataset, nil
}

// CreateVersion manages the creation of a version from a reader
func CreateVersion(reader io.Reader) (*Version, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}

	var version Version
	// Create unique id
	version.ID = uuid.NewV4().String()

	err = json.Unmarshal(b, &version)
	if err != nil {
		return nil, errors.New("Failed to parse json body")
	}

	return &version, nil
}

// CreateDownloadList manages the creation of a list downloadable items from a reader
func CreateDownloadList(reader io.Reader) (*DownloadList, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	var downloadList DownloadList
	if err := json.Unmarshal(b, &downloadList); err != nil {
		return nil, errors.Wrap(err, "failed to parse json to downloadList")
	}

	return &downloadList, nil
}

// CreateContact manages the creation of a contact from a reader
func CreateContact(reader io.Reader) (*Contact, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.New("Failed to read message body")
	}
	var contact Contact
	err = json.Unmarshal(b, &contact)
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
	case "":
		return errs.ErrVersionMissingState
	case EditionConfirmedState:
	case AssociatedState:
		hasAssociation = true
	case PublishedState:
		hasAssociation = true
	default:
		return errors.New("Incorrect state, can be one of the following: edition-confirmed, associated or published")
	}

	if hasAssociation && version.CollectionID == "" {
		return errors.New("Missing collection_id for association between version and a collection")
	}

	var missingFields []string
	var invalidFields []string

	if version.ReleaseDate == "" {
		missingFields = append(missingFields, "release_date")
	}

	if version.Downloads != nil {
		if version.Downloads.XLS != nil {
			if version.Downloads.XLS.URL == "" {
				missingFields = append(missingFields, "Downloads.XLS.URL")
			}
			if version.Downloads.XLS.Size == "" {
				missingFields = append(missingFields, "Downloads.XLS.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.XLS.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.XLS.Size not a number")
			}
		}

		if version.Downloads.CSV != nil {
			if version.Downloads.CSV.URL == "" {
				missingFields = append(missingFields, "Downloads.CSV.URL")
			}
			if version.Downloads.CSV.Size == "" {
				missingFields = append(missingFields, "Downloads.CSV.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.CSV.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.CSV.Size not a number")
			}
		}
	}

	if missingFields != nil {
		return fmt.Errorf("missing mandatory fields: %v", missingFields)
	}

	if invalidFields != nil {
		return fmt.Errorf("invalid fields: %v", invalidFields)
	}

	return nil
}
