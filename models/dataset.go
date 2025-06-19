package models

import (
	"context"
	//nolint:gosec // not used for secure purposes.

	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/v2/log"
)

// List of error variables
var (
	ErrEditionLinksInvalid = errors.New("editions links do not exist")
)

// DatasetType defines possible dataset types
type DatasetType int

// possible dataset types
const (
	Filterable DatasetType = iota
	CantabularTable
	CantabularBlob
	CantabularFlexibleTable
	CantabularMultivariateTable
	Static
	Invalid
)

var datasetTypes = []string{
	"filterable",
	"cantabular_table",
	"cantabular_blob",
	"cantabular_flexible_table",
	"cantabular_multivariate_table",
	"static",
	"invalid",
}

func (dt DatasetType) String() string {
	return datasetTypes[dt]
}

// GetDatasetType returns a dataset type for a given dataset
//
//nolint:goconst // "static" is part of a type definition slice
func GetDatasetType(datasetType string) (DatasetType, error) {
	switch datasetType {
	case "filterable", "v4", "":
		return Filterable, nil
	case "cantabular_table":
		return CantabularTable, nil
	case "cantabular_blob":
		return CantabularBlob, nil
	case "cantabular_flexible_table":
		return CantabularFlexibleTable, nil
	case "cantabular_multivariate_table":
		return CantabularMultivariateTable, nil
	case "static":
		return Static, nil
	default:
		return Invalid, errs.ErrDatasetTypeInvalid
	}
}

// DatasetUpdate represents an evolving dataset with the current dataset and the updated dataset
// Note: Stored as Dataset (in `dataset` Collection) in MongoDB
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
	LastUpdated       time.Time        `bson:"last_updated,omitempty"           json:"last_updated,omitempty"`
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
	Type              string           `bson:"type,omitempty"                   json:"type,omitempty"`
	IsBasedOn         *IsBasedOn       `bson:"is_based_on,omitempty"            json:"is_based_on,omitempty"`
	CanonicalTopic    string           `bson:"canonical_topic,omitempty"        json:"canonical_topic,omitempty"`
	Subtopics         []string         `bson:"subtopics,omitempty"              json:"subtopics,omitempty"`
	Survey            string           `bson:"survey,omitempty"                 json:"survey,omitempty"`
	RelatedContent    []GeneralDetails `bson:"related_content,omitempty"        json:"related_content,omitempty"`
	Topics            []string         `bson:"topics,omitempty"                 json:"topics,omitempty"`
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
	HRef string `bson:"href,omitempty"  json:"href,omitempty"`
	ID   string `bson:"id,omitempty"    json:"id,omitempty"`
}

// GeneralDetails represents generic fields stored against an object (reused)
type GeneralDetails struct {
	Description string `bson:"description,omitempty"    json:"description,omitempty"`
	HRef        string `bson:"href,omitempty"           json:"href,omitempty"`
	Title       string `bson:"title,omitempty"          json:"title,omitempty"`
}

// Contact represents information of individual contact details
type Contact struct {
	Email       string    `bson:"email,omitempty"          json:"email,omitempty"`
	ID          string    `bson:"_id,omitempty"            json:"id,omitempty"`
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
	LatestVersion *LinkObject `bson:"latest_version,omitempty" json:"latest_version,omitempty"`
	Self          *LinkObject `bson:"self,omitempty"           json:"self,omitempty"`
	Versions      *LinkObject `bson:"versions,omitempty"       json:"versions,omitempty"`
}

// Edition represents information related to a single edition for a dataset
type Edition struct {
	Edition            string              `bson:"edition,omitempty"             json:"edition,omitempty"`
	EditionTitle       string              `bson:"edition_title,omitempty"       json:"edition_title,omitempty"`
	ID                 string              `bson:"id,omitempty"                  json:"id,omitempty"`
	DatasetID          string              `bson:"dataset_id,omitempty"          json:"dataset_id,omitempty"`
	Version            int                 `bson:"version,omitempty"             json:"version,omitempty"`
	LastUpdated        time.Time           `bson:"last_updated,omitempty"        json:"-"`
	ReleaseDate        string              `bson:"release_date,omitempty"        json:"release_date,omitempty"`
	Links              *EditionUpdateLinks `bson:"links,omitempty"               json:"links,omitempty"`
	State              string              `bson:"state,omitempty"               json:"state,omitempty"`
	Alerts             *[]Alert            `bson:"alerts,omitempty"              json:"alerts,omitempty"`
	UsageNotes         *[]UsageNote        `bson:"usage_notes,omitempty"         json:"usage_notes,omitempty"`
	Distributions      *[]Distribution     `bson:"distributions,omitempty"       json:"distributions,omitempty"`
	IsBasedOn          *IsBasedOn          `bson:"is_based_on,omitempty"         json:"is_based_on,omitempty"`
	Type               string              `bson:"type,omitempty"                json:"type,omitempty"`
	QualityDesignation QualityDesignation  `bson:"quality_designation,omitempty" json:"quality_designation,omitempty"`
}

// DatasetEdition represents a dataset edition
type DatasetEdition struct {
	DatasetID     string     `json:"dataset_id"`
	Title         string     `json:"title"`
	Edition       string     `json:"edition"`
	EditionTitle  string     `json:"edition_title"`
	LatestVersion LinkObject `json:"latest_version"`
	ReleaseDate   string     `json:"release_date"`
}

// Publisher represents an object containing information of the publisher
type Publisher struct {
	HRef string `bson:"href,omitempty" json:"href,omitempty"`
	Name string `bson:"name,omitempty" json:"name,omitempty"`
	Type string `bson:"type,omitempty" json:"type,omitempty"`
}

// IsBasedOn refers to the Cantabular blob source
type IsBasedOn struct {
	Type string `bson:"type" json:"@type"`
	ID   string `bson:"id"   json:"@id"`
}

// CreateDataset manages the creation of a dataset from a reader
func CreateDataset(reader io.Reader) (*Dataset, error) {
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var dataset Dataset

	err = json.Unmarshal(b, &dataset)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
	}

	return &dataset, nil
}

// CreateContact manages the creation of a contact from a reader
func CreateContact(reader io.Reader) (*Contact, error) {
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}
	var contact Contact
	err = json.Unmarshal(b, &contact)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	// Create unique id
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	contact.ID = id.String()

	return &contact, nil
}

// CreateEdition manages the creation of an edition object
func CreateEdition(host, datasetID, edition string) (*EditionUpdate, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	return &EditionUpdate{
		ID: id.String(),
		Next: &Edition{
			Edition: edition,
			State:   EditionConfirmedState,
			Links: &EditionUpdateLinks{
				Dataset: &LinkObject{
					ID:   datasetID,
					HRef: fmt.Sprintf("%s/datasets/%s", host, datasetID),
				},
				Self: &LinkObject{
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", host, datasetID, edition),
				},
				Versions: &LinkObject{
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions", host, datasetID, edition),
				},
				LatestVersion: &LinkObject{
					ID:   "1",
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/1", host, datasetID, edition),
				},
			},
		},
	}, nil
}

// UpdateLinks in the editions.next document, ensuring links can't regress once published to current
func (ed *EditionUpdate) UpdateLinks(ctx context.Context, host string) error {
	if ed.Next == nil || ed.Next.Links == nil || ed.Next.Links.LatestVersion == nil || ed.Next.Links.LatestVersion.ID == "" {
		return ErrEditionLinksInvalid
	}

	versionID := ed.Next.Links.LatestVersion.ID
	version, err := strconv.Atoi(ed.Next.Links.LatestVersion.ID)
	if err != nil {
		return errors.Wrap(err, "failed to convert version id from edition.next document")
	}

	currentVersion := 0

	if ed.Current != nil && ed.Current.Links != nil && ed.Current.Links.LatestVersion != nil {
		var err error
		currentVersion, err = strconv.Atoi(ed.Current.Links.LatestVersion.ID)
		if err != nil {
			return errors.Wrap(err, "failed to convert version id from edition.current document")
		}
	}

	if currentVersion > version {
		log.Info(ctx, "published edition links to a higher version than the requested change", log.Data{"doc": ed, "versionID": versionID})
		return errors.New("published edition links to a higher version than the requested change")
	}

	version++
	versionID = strconv.Itoa(version)

	ed.Next.Links.LatestVersion = &LinkObject{
		ID:   versionID,
		HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", host, ed.Next.Links.Dataset.ID, ed.Next.Edition, versionID),
	}

	return nil
}

// PublishLinks applies the provided versionLink object to the edition being published only
// if that version is greater than the latest published version
func (ed *EditionUpdate) PublishLinks(ctx context.Context, versionLink *LinkObject) error {
	if ed.Next == nil || ed.Next.Links == nil || ed.Next.Links.LatestVersion == nil {
		return errors.New("editions links do not exist")
	}

	currentVersion := 0

	if ed.Current != nil && ed.Current.Links != nil && ed.Current.Links.LatestVersion != nil {
		var err error
		currentVersion, err = strconv.Atoi(ed.Current.Links.LatestVersion.ID)
		if err != nil {
			return fmt.Errorf("failed to parse LatestVersion.ID: %w", err)
		}
	}

	if versionLink == nil {
		return errors.New("invalid arguments to PublishLinks - versionLink empty")
	}

	version, err := strconv.Atoi(versionLink.ID)
	if err != nil {
		return fmt.Errorf("failed to parse VersionLink.ID: %w", err)
	}

	if currentVersion > version {
		log.Info(ctx, "current latest version is higher, no edition update required", log.Data{"doc": ed, "currentVersionID": currentVersion, "versionID": versionLink.ID})
		return nil
	}

	ed.Next.Links.LatestVersion = versionLink
	return nil
}

// CleanDataset trims URI and any hrefs contained in the database
func CleanDataset(dataset *Dataset) {
	dataset.URI = strings.TrimSpace(dataset.URI)

	if dataset.QMI != nil {
		dataset.QMI.HRef = strings.TrimSpace(dataset.QMI.HRef)
	}

	if dataset.Publisher != nil {
		dataset.Publisher.HRef = strings.TrimSpace(dataset.Publisher.HRef)
	}

	for i := range dataset.Publications {
		dataset.Publications[i].HRef = strings.TrimSpace(dataset.Publications[i].HRef)
	}

	for i := range dataset.Methodologies {
		dataset.Methodologies[i].HRef = strings.TrimSpace(dataset.Methodologies[i].HRef)
	}

	for i := range dataset.RelatedDatasets {
		dataset.RelatedDatasets[i].HRef = strings.TrimSpace(dataset.RelatedDatasets[i].HRef)
	}
}

// ValidateDataset checks the dataset has invalid fields
func ValidateDataset(dataset *Dataset) error {
	var invalidFields []string

	if dataset.Type == "static" {
		mandatoryStringFields := map[string]string{
			"ID":          dataset.ID,
			"Title":       dataset.Title,
			"Description": dataset.Description,
			"NextRelease": dataset.NextRelease,
			"License":     dataset.License,
		}

		for fieldName, fieldValue := range mandatoryStringFields {
			if fieldValue == "" {
				invalidFields = append(invalidFields, fieldName)
			}
		}

		if len(dataset.Keywords) == 0 {
			invalidFields = append(invalidFields, "Keywords")
		}

		if len(dataset.Contacts) == 0 {
			invalidFields = append(invalidFields, "Contacts")
		}

		if len(dataset.Topics) == 0 {
			invalidFields = append(invalidFields, "Topics")
		}
	}

	if dataset.URI != "" {
		invalidFields = append(invalidFields, validateURLString(dataset.URI, "URI")...)
	}

	if dataset.QMI != nil && dataset.QMI.HRef != "" {
		invalidFields = append(invalidFields, validateURLString(dataset.QMI.HRef, "QMI")...)
	}

	if dataset.Publisher != nil && dataset.Publisher.HRef != "" {
		invalidFields = append(invalidFields, validateURLString(dataset.Publisher.HRef, "Publisher")...)
	}

	invalidFields = append(invalidFields, validateGeneralDetails(dataset.Publications, "Publications")...)

	invalidFields = append(invalidFields, validateGeneralDetails(dataset.RelatedDatasets, "RelatedDatasets")...)

	invalidFields = append(invalidFields, validateGeneralDetails(dataset.Methodologies, "Methodologies")...)

	if len(invalidFields) > 0 {
		return fmt.Errorf("invalid fields: %v", invalidFields)
	}

	return nil
}

func validateGeneralDetails(generalDetails []GeneralDetails, identifier string) (invalidFields []string) {
	for i, gd := range generalDetails {
		invalidFields = append(invalidFields, validateURLString(gd.HRef, fmt.Sprintf("%s[%d].HRef", identifier, i))...)
	}
	return
}

func validateURLString(urlString, identifier string) (invalidFields []string) {
	u, err := url.Parse(urlString)
	if err != nil || (u.Scheme != "" && u.Host == "" && u.Path == "") || (u.Scheme != "" && u.Host == "" && u.Path != "") {
		invalidFields = append(invalidFields, identifier)
	}
	return
}

// ValidateDatasetType checks the dataset.type field has valid type
func ValidateDatasetType(ctx context.Context, datasetType string) (*DatasetType, error) {
	dataType, err := GetDatasetType(datasetType)
	if err != nil {
		log.Error(ctx, "error Invalid dataset type", err)
		return nil, err
	}
	return &dataType, nil
}
