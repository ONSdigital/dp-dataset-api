package models

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// DatasetType defines possible dataset types
type DatasetType int

// possible dataset types
const (
	Filterable DatasetType = iota
	Nomis
	CantabularTable
	CantabularBlob
	Invalid
)

var datasetTypes = []string{"filterable", "nomis", "cantabular_table", "cantabular_blob", "invalid"}

func (dt DatasetType) String() string {
	return datasetTypes[dt]
}

// GetDatasetType returns a dataset type for a given dataset
func GetDatasetType(datasetType string) (DatasetType, error) {
	switch datasetType {
	case "filterable", "":
		return Filterable, nil
	case "nomis":
		return Nomis, nil
	case "cantabular_table":
		return CantabularTable, nil
	case "cantabular_blob":
		return CantabularBlob, nil
	default:
		return Invalid, errs.ErrDatasetTypeInvalid
	}
}

// List of error variables
var (
	ErrAssociatedVersionCollectionIDInvalid = errors.New("missing collection_id for association between version and a collection")
	ErrPublishedVersionCollectionIDInvalid  = errors.New("unexpected collection_id in published version")
	ErrVersionStateInvalid                  = errors.New("incorrect state, can be one of the following: edition-confirmed, associated or published")
	ErrEditionLinksInvalid                  = errors.New("editions links do not exist")
)

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
	LastUpdated       time.Time        `bson:"last_updated,omitempty"           json:"-"`
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
	NomisReferenceURL string           `bson:"nomis_reference_url,omitempty"    json:"nomis_reference_url,omitempty"`
	IsBasedOn         *IsBasedOn       `bson:"is_based_on,omitempty"            json:"is_based_on,omitempty"`
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
	Edition     string              `bson:"edition,omitempty"      json:"edition,omitempty"`
	ID          string              `bson:"id,omitempty"           json:"id,omitempty"`
	LastUpdated time.Time           `bson:"last_updated,omitempty" json:"-"`
	Links       *EditionUpdateLinks `bson:"links,omitempty"        json:"links,omitempty"`
	State       string              `bson:"state,omitempty"        json:"state,omitempty"`
	IsBasedOn   *IsBasedOn          `bson:"is_based_on,omitempty"  json:"is_based_on,omitempty"`
	Type        string              `bson:"type,omitempty"         json:"type,omitempty"`
}

// Publisher represents an object containing information of the publisher
type Publisher struct {
	HRef string `bson:"href,omitempty" json:"href,omitempty"`
	Name string `bson:"name,omitempty" json:"name,omitempty"`
	Type string `bson:"type,omitempty" json:"type,omitempty"`
}

// Version represents information related to a single version for an edition of a dataset
type Version struct {
	Alerts        *[]Alert             `bson:"alerts,omitempty"         json:"alerts,omitempty"`
	CollectionID  string               `bson:"collection_id,omitempty"  json:"collection_id,omitempty"`
	DatasetID     string               `bson:"-"                        json:"dataset_id,omitempty"`
	Dimensions    []Dimension          `bson:"dimensions,omitempty"     json:"dimensions,omitempty"`
	Downloads     *DownloadList        `bson:"downloads,omitempty"      json:"downloads,omitempty"`
	Edition       string               `bson:"edition,omitempty"        json:"edition,omitempty"`
	Headers       []string             `bson:"headers,omitempty"        json:"-"`
	ID            string               `bson:"id,omitempty"             json:"id,omitempty"`
	LastUpdated   time.Time            `bson:"last_updated,omitempty"   json:"-"`
	LatestChanges *[]LatestChange      `bson:"latest_changes,omitempty" json:"latest_changes,omitempty"`
	Links         *VersionLinks        `bson:"links,omitempty"          json:"links,omitempty"`
	ReleaseDate   string               `bson:"release_date,omitempty"   json:"release_date,omitempty"`
	State         string               `bson:"state,omitempty"          json:"state,omitempty"`
	Temporal      *[]TemporalFrequency `bson:"temporal,omitempty"       json:"temporal,omitempty"`
	UsageNotes    *[]UsageNote         `bson:"usage_notes,omitempty"    json:"usage_notes,omitempty"`
	Version       int                  `bson:"version,omitempty"        json:"version,omitempty"`
}

// Alert represents an object containing information on an alert
type Alert struct {
	Date        string `bson:"date,omitempty"        json:"date,omitempty"`
	Description string `bson:"description,omitempty" json:"description,omitempty"`
	Type        string `bson:"type,omitempty"        json:"type,omitempty"`
}

// DownloadList represents a list of objects of containing information on the downloadable files
type DownloadList struct {
	CSV  *DownloadObject `bson:"csv,omitempty" json:"csv,omitempty"`
	CSVW *DownloadObject `bson:"csvw,omitempty" json:"csvw,omitempty"`
	XLS  *DownloadObject `bson:"xls,omitempty" json:"xls,omitempty"`
}

// DownloadObject represents information on the downloadable file
type DownloadObject struct {
	HRef    string `bson:"href,omitempty"  json:"href,omitempty"`
	Private string `bson:"private,omitempty" json:"private,omitempty"`
	Public  string `bson:"public,omitempty" json:"public,omitempty"`
	// TODO size is in bytes and probably should be an int64 instead of a string this
	// will have to change for several services (filter API, exporter services and web)
	Size string `bson:"size,omitempty" json:"size,omitempty"`
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

// UsageNote represents a note containing extra information associated to the resource
type UsageNote struct {
	Note  string `bson:"note,omitempty"     json:"note,omitempty"`
	Title string `bson:"title,omitempty"    json:"title,omitempty"`
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

// IsBasedOn refers to the Cantabular blob source
type IsBasedOn struct {
	Type string `bson:"@type" json:"@type"`
	ID   string `bson:"@id"   json:"@id"`
}

// CreateDataset manages the creation of a dataset from a reader
func CreateDataset(reader io.Reader) (*Dataset, error) {
	b, err := ioutil.ReadAll(reader)
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

// CreateVersion manages the creation of a version from a reader
func CreateVersion(reader io.Reader, datasetID string) (*Version, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var version Version
	version.ID = uuid.NewV4().String()
	version.DatasetID = datasetID

	err = json.Unmarshal(b, &version)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
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
		return nil, errs.ErrUnableToReadMessage
	}
	var contact Contact
	err = json.Unmarshal(b, &contact)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	// Create unique id
	id := uuid.NewV4()
	contact.ID = id.String()

	return &contact, nil
}

// CreateEdition manages the creation of a an edition object
func CreateEdition(host, datasetID, edition string) (*EditionUpdate, error) {
	id := uuid.NewV4()

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

//UpdateLinks in the editions.next document, ensuring links can't regress once published to current
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

//PublishLinks applies the provided versionLink object to the edition being published only
//if that version is greater than the latest published version
func (ed *EditionUpdate) PublishLinks(ctx context.Context, host string, versionLink *LinkObject) error {
	if ed.Next == nil || ed.Next.Links == nil || ed.Next.Links.LatestVersion == nil {
		return errors.New("editions links do not exist")
	}

	currentVersion := 0

	if ed.Current != nil && ed.Current.Links != nil && ed.Current.Links.LatestVersion != nil {
		var err error
		currentVersion, err = strconv.Atoi(ed.Current.Links.LatestVersion.ID)
		if err != nil {
			return err
		}
	}

	if versionLink == nil {
		return errors.New("invalid arguments to PublishLinks - versionLink empty")
	}

	version, err := strconv.Atoi(versionLink.ID)
	if err != nil {
		return err
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
	if dataset.URI != "" {
		invalidFields = append(invalidFields, validateUrlString(dataset.URI, "URI")...)
	}

	if dataset.QMI != nil && dataset.QMI.HRef != "" {
		invalidFields = append(invalidFields, validateUrlString(dataset.QMI.HRef, "QMI")...)
	}

	if dataset.Publisher != nil && dataset.Publisher.HRef != "" {
		invalidFields = append(invalidFields, validateUrlString(dataset.Publisher.HRef, "Publisher")...)
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
		invalidFields = append(invalidFields, validateUrlString(gd.HRef, fmt.Sprintf("%s[%d].HRef", identifier, i))...)
	}
	return
}

func validateUrlString(urlString string, identifier string) (invalidFields []string) {
	url, err := url.Parse(urlString)
	if err != nil || (url.Scheme != "" && url.Host == "" && url.Path == "") || (url.Scheme != "" && url.Host == "" && url.Path != "") {
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

// ValidateNomisURL checks for the nomis type when the dataset has nomis URL
func ValidateNomisURL(ctx context.Context, datasetType string, nomisURL string) (string, error) {

	if nomisURL != "" && datasetType != Nomis.String() {
		log.Error(ctx, "error Type mismatch", errs.ErrDatasetTypeInvalid)
		return "", errs.ErrTypeMismatch
	}
	return datasetType, nil
}

// ValidateVersion checks the content of the version structure
func ValidateVersion(version *Version) error {

	switch version.State {
	case "":
		return errs.ErrVersionMissingState
	case EditionConfirmedState:
	case PublishedState:
		if version.CollectionID != "" {
			return ErrPublishedVersionCollectionIDInvalid
		}
	case AssociatedState:
		if version.CollectionID == "" {
			return ErrAssociatedVersionCollectionIDInvalid
		}
	default:
		return ErrVersionStateInvalid
	}

	var missingFields []string
	var invalidFields []string

	if version.ReleaseDate == "" {
		missingFields = append(missingFields, "release_date")
	}

	if version.Downloads != nil {
		if version.Downloads.XLS != nil {
			if version.Downloads.XLS.HRef == "" {
				missingFields = append(missingFields, "Downloads.XLS.HRef")
			}
			if version.Downloads.XLS.Size == "" {
				missingFields = append(missingFields, "Downloads.XLS.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.XLS.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.XLS.Size not a number")
			}
		}

		if version.Downloads.CSV != nil {
			if version.Downloads.CSV.HRef == "" {
				missingFields = append(missingFields, "Downloads.CSV.HRef")
			}
			if version.Downloads.CSV.Size == "" {
				missingFields = append(missingFields, "Downloads.CSV.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.CSV.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.CSV.Size not a number")
			}
		}

		if version.Downloads.CSVW != nil {
			if version.Downloads.CSVW.HRef == "" {
				missingFields = append(missingFields, "Downloads.CSVW.HRef")
			}
			if version.Downloads.CSVW.Size == "" {
				missingFields = append(missingFields, "Downloads.CSVW.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.CSVW.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.CSVW.Size not a number")
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

// ValidateVersionNumber checks the version is a positive integer above 0
func ValidateVersionNumber(ctx context.Context, version string) (int, error) {
	versionNumber, err := strconv.Atoi(version)
	if err != nil {
		log.Error(ctx, "invalid version provided", err, log.Data{"version": version})
		return versionNumber, errs.ErrInvalidVersion
	}

	if !(versionNumber > 0) {
		log.Error(ctx, "version is not a positive integer", errs.ErrInvalidVersion, log.Data{"version": version})
		return versionNumber, errs.ErrInvalidVersion
	}

	return versionNumber, nil
}
