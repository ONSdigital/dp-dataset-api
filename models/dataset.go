package models

import (
	"context"
	//nolint:gosec // not used for secure purposes.
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"go.mongodb.org/mongo-driver/bson"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/v2/log"
)

// DatasetType defines possible dataset types
type DatasetType int

// possible dataset types
const (
	Static DatasetType = iota
	CantabularTable
	CantabularBlob
	CantabularFlexibleTable
	CantabularMultivariateTable
	Invalid
)

var datasetTypes = []string{
	"static",
	"cantabular_table",
	"cantabular_blob",
	"cantabular_flexible_table",
	"cantabular_multivariate_table",
	"invalid",
}

func (dt DatasetType) String() string {
	return datasetTypes[dt]
}

// GetDatasetType returns a dataset type for a given dataset
func GetDatasetType(datasetType string) (DatasetType, error) {
	switch datasetType {
	case "static":
		return Static, nil
	case "cantabular_table":
		return CantabularTable, nil
	case "cantabular_blob":
		return CantabularBlob, nil
	case "cantabular_flexible_table":
		return CantabularFlexibleTable, nil
	case "cantabular_multivariate_table":
		return CantabularMultivariateTable, nil
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

type LinkedData struct {
	Context string   `json:"@context,omitempty"`
	ID      string   `json:"@id,omitempty"`
	Type    []string `json:"@type,omitempty" groups:"dataset,edition"`
}

type DatasetList struct {
	Page       `groups:"datasets"`
	Links      *PageLinks   `json:"_links" groups:"datasets"`
	Items      []*LDDataset `json:"items" groups:"datasets"`
	LinkedData `groups:"all"`
}

// LDDataset represents information related to a single dataset
type LDDataset struct {
	// HAL and Internal fields
	ETag        string    `bson:"e_tag,omitempty"  json:"-"`
	LastUpdated time.Time `bson:"last_updated" json:"-"`

	CollectionID   string `bson:"collection_id,omitempty"          json:"collection_id,omitempty" groups:"datasets,dataset"`
	State          string `bson:"state,omitempty"                  json:"state,omitempty" groups:"datasets,dataset"`
	CanonicalTopic string `bson:"canonical_topic,omitempty"        json:"canonical_topic,omitempty" groups:"datasets,dataset"`

	Links    *LDDatasetLinks  `bson:"-" json:"_links,omitempty" groups:"all"`
	Embedded *DatasetEmbedded `bson:"-" json:"_embedded,omitempty" groups:"dataset"`

	// JSON-LD and Application Profile fields
	LinkedData        `bson:"-" groups:"all"`
	DCATDatasetSeries `bson:",inline"`
}

// DCATDatasetSeries represents all the fields specified in the Application Profile for a dcat:DatasetSeries
// These fields are also all needed for a dcat:Dataset representation, which is used for Editions and Versions
type DCATDatasetSeries struct {
	Identifier string `bson:"_id,omitempty"  json:"identifier,omitempty" groups:"all"`

	// TODO: Add these fields to AP or relocate to Dataset struct
	ContactPoint      *ContactDetails `bson:"contact_point,omitempty"          json:"contact_point,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	NationalStatistic *bool           `bson:"national_statistic,omitempty"     json:"national_statistic,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	IsBasedOn         *IsBasedOn      `bson:"is_based_on,omitempty"            json:"is_based_on,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	Survey            string          `bson:"survey,omitempty"                 json:"survey,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	DatasetType       string          `bson:"dataset_type,omitempty"           json:"dataset_type,omitempty" groups:"datasets,dataset,edition,version,instance"`

	//Descriptive - omitted fields from AP: 'created' time, 'creator', 'label'
	Publisher *ContactDetails `bson:"publisher,omitempty"        json:"publisher,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	Modified  time.Time       `bson:"modified,omitempty"         json:"modified,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	Issued    time.Time       `bson:"issued,omitempty"           json:"issued" groups:"datasets,dataset,editions,edition,versions,version,instance"` //add to spec
	Title     string          `bson:"title,omitempty"            json:"title,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`

	//Summary
	Keywords    []string `bson:"keywords,omitempty"       json:"keywords,omitempty" groups:"dataset,edition,version,instance"`
	Themes      []string `bson:"themes,omitempty"         json:"themes,omitempty" groups:"dataset,edition,version,instance"`
	Description string   `bson:"description,omitempty"    json:"description,omitempty" groups:"dataset,edition,version,instance"`

	Frequency string `bson:"frequency,omitempty"      json:"frequency,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
	Summary   string `bson:"summary,omitempty"        json:"summary,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"` //shorter than description
	License   string `bson:"license,omitempty"        json:"license,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`

	//Scope
	SpatialCoverage    string   `bson:"spatial_coverage,omitempty"    json:"spatial_coverage,omitempty" groups:"dataset,edition,version,instance"`
	SpatialResolution  []string `bson:"spatial_resolution,omitempty"  json:"spatial_resolution,omitempty" groups:"dataset,edition,version,instance"`
	TemporalCoverage   string   `bson:"temporal_coverage,omitempty"   json:"temporal_coverage,omitempty" groups:"dataset,edition,version,instance"`
	TemporalResolution []string `bson:"temporal_resolution,omitempty" json:"temporal_resolution,omitempty" groups:"dataset,edition,version,instance"`

	//Management - omitted fields from AP: first, last. These should be provided by the _embedded fields on the response
	NextRelease string `bson:"next_release,omitempty"           json:"next_release,omitempty" groups:"datasets,dataset,editions,edition,versions,version,instance"`
}

type EditionList struct {
	Page       `groups:"editions,versions"`
	Links      *PageLinks   `json:"_links" groups:"editions,versions"`
	Items      []*LDEdition `json:"items" groups:"editions,versions"`
	LinkedData `groups:"all"`
}

// LDEdition ...
type LDEdition struct {
	// HAL and Internal fields
	ETag        string    `bson:"e_tag"   json:"-"`
	LastUpdated time.Time `bson:"last_updated" json:"-"`

	CollectionID string           `bson:"collection_id,omitempty"          json:"collection_id,omitempty" groups:"editions,edition,versions,version,instances,instance"`
	State        string           `bson:"state,omitempty"                  json:"state,omitempty" groups:"editions,edition,versions,version,instances,instance"`
	Links        *EditionLinks    `bson:"_links" json:"_links,omitempty" groups:"editions,edition,versions,version"`
	Embedded     *EditionEmbedded `bson:"-" json:"_embedded,omitempty" groups:"edition,version,instance"`

	// JSON-LD and Application Profile fields
	LinkedData  `bson:"-" groups:"all"`
	DCATDataset `bson:",inline"`
}

type DCATDataset struct {
	DCATDatasetSeries `bson:"-"`

	// Quality
	VersionNotes []string  `bson:"version_notes,omitempty"   json:"version_notes,omitempty" groups:"editions,edition,versions,version,instances,instance"`
	ReleaseDate  time.Time `bson:"release_date,omitempty"           json:"release_date" groups:"editions,edition,versions,version,instances,instance"` //add to spec

	// Management
	Edition string `bson:"edition,omitempty"           json:"edition,omitempty" groups:"editions,edition,versions,version"`
	Version int    `bson:"version,omitempty"           json:"version,omitempty" groups:"editions,edition,versions,version"`

	NextEdition     string `bson:"next_edition,omitempty"           json:"next_edition,omitempty" groups:"edition"`
	PreviousEdition string `bson:"previous_edition,omitempty"           json:"previous_edition,omitempty" groups:"edition"`

	NextVersion     string `bson:"next_version,omitempty"           json:"next_version,omitempty" groups:"version"`
	PreviousVersion string `bson:"previous_version,omitempty"           json:"previous_version,omitempty" groups:"version"`

	// Distributions - these are currently embedded in the Edition doc instead of here
	//Distributions []*Distribution `bson:"distributions,omitempty"           json:"disributions,omitempty" groups:"editions,edition"`
}

type Distribution struct {
	ETag string `bson:"e_tag,omitempty"  json:"-,omitempty"`

	Type string `bson:"media_type,omitempty" json:"media_type,omitempty" groups:"editions,edition,distributions"`
	URL  string `bson:"download_url,omitempty"  json:"download_url,omitempty" groups:"editions,edition,distributions"`
	Size string `bson:"byte_size,omitempty" json:"byte_size,omitempty" groups:"editions,edition,distributions"` //AP says no editions, Website says yes

	// These fields should only exist for CSV static datasets
	Checksum string `bson:"checksum,omitempty"  json:"checksum,omitempty" groups:"editions,edition,distributions"`

	// Relationships - omitted fields from AP: derivedFrom, generatedBy
	DescribedBy string `bson:"described_by,omitempty" json:"described_by,omitempty" groups:"edition,distributions"` //AP says yes editions, embedding the smaller portion for now

	// Schema
	Schema *Schema `bson:"table_schema,omitempty" json:"table_schema,omitempty" groups:"edition,distributions"` //AP says yes editions, embedding the smaller portion for now
}

type Schema struct {
	AboutURL string    `bson:"about_url,omitempty"  json:"about_url,omitempty" groups:"edition,distributions"`
	Columns  []*Column `bson:"columns,omitempty"  json:"columns,omitempty" groups:"edition,distributions"` //AP says yes editions, embedding the smaller portion for now
}

type Column struct {
	// omitted fields from AP: propertyURL, value URL, description, label

	ComponentType string `bson:"component_type,omitempty"  json:"component_type,omitempty" groups:"edition,distributions"` //AP says yes editions, embedding the smaller portion for now
	Name          string `bson:"name,omitempty"  json:"name,omitempty" groups:"edition,distributions"`                     //AP says yes editions, embedding the smaller portion for now
	DataType      string `bson:"data_type,omitempty"  json:"data_type,omitempty" groups:"edition,distributions"`
	Title         string `bson:"title,omitempty"  json:"title,omitempty" groups:"edition,distributions"`
}

// LDDatasetLinks ...
type LDDatasetLinks struct { //TODO - can potentially remove bson tags here as its "-" in the only usage
	Editions      *LinkObject `bson:"-"        json:"editions,omitempty" groups:"dataset"`
	LatestVersion *LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty" groups:"datasets,dataset"`
	Self          *LinkObject `bson:"-"            json:"self,omitempty" groups:"datasets,dataset"`
}

// DatasetEmbedded ...
type DatasetEmbedded struct {
	Editions []EmbeddedEdition `json:"editions,omitempty" groups:"dataset"`
}

type EmbeddedEdition struct {
	ID     string    `bson:"_id" json:"@id"`
	Issued time.Time `bson:"issued" json:"issued"`
	ETag   string    `bson:"e_tag"                      json:"etag"`
}

// EditionLinks ...
type EditionLinks struct {
	Dataset       *LinkObject `bson:"dataset,omitempty"        json:"dataset,omitempty" groups:"edition,version,instances,instance"`
	Editions      *LinkObject `bson:"-" json:"editions,omitempty" groups:"edition"`
	Edition       *LinkObject `bson:"edition,omitempty"        json:"edition,omitempty" groups:"version,instances,instance"`
	Versions      *LinkObject `bson:"-" json:"versions,omitempty" groups:"edition"`
	Dimensions    *LinkObject `bson:"-" json:"dimensions,omitempty" groups:"versions,version"`
	Distributions *LinkObject `bson:"-" json:"distributions,omitempty" groups:"versions,version"`
	LatestVersion *LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty" groups:"edition"`
	Next          *LinkObject `bson:"next,omitempty"        json:"next,omitempty" groups:"edition,version"`
	Prev          *LinkObject `bson:"prev,omitempty"        json:"prev,omitempty" groups:"edition,version"`
	Self          *LinkObject `bson:"self,omitempty"            json:"self,omitempty" groups:"editions,edition,versions,version,instances,instance"`
}

// EditionEmbedded ...
type EditionEmbedded struct {
	Versions      []EmbeddedVersion   `groups:"edition"`
	Dimensions    []EmbeddedDimension `groups:"version"`
	Distributions []*Distribution     `groups:"edition,version"`
}

type EmbeddedVersion struct {
	ID           string    `json:"@id"`
	ReleaseDate  time.Time `bson:"release_date" json:"release_date"`
	ETag         string    `bson:"e_tag,omitempty" json:"etag"`
	VersionNotes []string  `bson:"version_notes,omitempty" json:"version_notes,omitempty"`
	Version      int       `bson:"version" json:"version,omitempty"`
}

type EmbeddedDimension struct {
	CodeList   string `json:"code_list"`
	Identifier string `json:"identifier"`
	Label      string `json:"label"`
	Name       string `json:"name"`
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
	CanonicalTopic    string           `bson:"canonical_topic,omitempty"        json:"canonical_topic,omitempty"`
	Subtopics         []string         `bson:"subtopics,omitempty"              json:"subtopics,omitempty"`
	Survey            string           `bson:"survey,omitempty"                 json:"survey,omitempty"`
	RelatedContent    []GeneralDetails `bson:"related_content,omitempty"        json:"related_content,omitempty"`
}

// DatasetLinks represents a list of specific links related to the dataset resource
type DatasetLinks struct {
	AccessRights  *LinkObject `bson:"access_rights,omitempty"   json:"access_rights,omitempty"`
	Editions      *LinkObject `bson:"editions,omitempty"        json:"editions,omitempty"`
	LatestVersion *LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty"`
	Self          *LinkObject `bson:"self,omitempty"            json:"self,omitempty"`
	Taxonomy      *LinkObject `bson:"taxonomy,omitempty"        json:"taxonomy,omitempty"`
}

// GeneralDetails represents generic fields stored against an object (reused)
type GeneralDetails struct {
	Description string `bson:"description,omitempty"    json:"description,omitempty"`
	HRef        string `bson:"href,omitempty"           json:"href,omitempty"`
	Title       string `bson:"title,omitempty"          json:"title,omitempty"`
}

// Contact represents information of individual contact details
// type Contact struct {
// 	Email       string    `bson:"email,omitempty"          json:"email,omitempty"`
// 	ID          string    `bson:"_id,omitempty"            json:"id,omitempty"`
// 	LastUpdated time.Time `bson:"last_updated,omitempty"   json:"-"`
// 	Name        string    `bson:"name,omitempty"           json:"name,omitempty"`
// 	Telephone   string    `bson:"telephone,omitempty"      json:"telephone,omitempty"`
// }

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
	Alerts          *[]Alert             `bson:"alerts,omitempty"           json:"alerts,omitempty"`
	CollectionID    string               `bson:"collection_id,omitempty"    json:"collection_id,omitempty"`
	DatasetID       string               `bson:"-"                          json:"dataset_id,omitempty"`
	Dimensions      []Dimension          `bson:"dimensions,omitempty"       json:"dimensions,omitempty"`
	Downloads       *DownloadList        `bson:"downloads,omitempty"        json:"downloads,omitempty"`
	Edition         string               `bson:"edition,omitempty"          json:"edition,omitempty"`
	Headers         []string             `bson:"headers,omitempty"          json:"-"`
	ID              string               `bson:"id,omitempty"               json:"id,omitempty"`
	LastUpdated     time.Time            `bson:"last_updated,omitempty"     json:"-"`
	LatestChanges   *[]LatestChange      `bson:"latest_changes,omitempty"   json:"latest_changes,omitempty"`
	Links           *VersionLinks        `bson:"links,omitempty"            json:"links,omitempty"`
	ReleaseDate     string               `bson:"release_date,omitempty"     json:"release_date,omitempty"`
	State           string               `bson:"state,omitempty"            json:"state,omitempty"`
	Temporal        *[]TemporalFrequency `bson:"temporal,omitempty"         json:"temporal,omitempty"`
	UsageNotes      *[]UsageNote         `bson:"usage_notes,omitempty"      json:"usage_notes,omitempty"`
	IsBasedOn       *IsBasedOn           `bson:"is_based_on,omitempty"      json:"is_based_on,omitempty"`
	Version         int                  `bson:"version,omitempty"          json:"version,omitempty"`
	Type            string               `bson:"type,omitempty"             json:"type,omitempty"`
	ETag            string               `bson:"e_tag"                      json:"-"`
	LowestGeography string               `bson:"lowest_geography,omitempty" json:"lowest_geography,omitempty"`
}

// Hash generates a SHA-1 hash of the version struct. SHA-1 is not cryptographically safe,
// but it has been selected for performance as we are only interested in uniqueness.
// ETag field value is ignored when generating a hash.
// An optional byte array can be provided to append to the hash.
// This can be used, for example, to calculate a hash of this version and an update applied to it.
func (v *Version) Hash(extraBytes []byte) (string, error) {
	//nolint:gosec // sha1 not used for secure purposes
	h := sha1.New()

	// copy by value to ignore ETag without affecting v
	v2 := *v
	v2.ETag = ""

	versionBytes, err := bson.Marshal(v2)
	if err != nil {
		return "", err
	}

	if _, err := h.Write(append(versionBytes, extraBytes...)); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Alert represents an object containing information on an alert
type Alert struct {
	Date        string `bson:"date,omitempty"        json:"date,omitempty"`
	Description string `bson:"description,omitempty" json:"description,omitempty"`
	Type        string `bson:"type,omitempty"        json:"type,omitempty"`
}

// DownloadList represents a list of objects of containing information on the downloadable files.
// Items are in a specific order and should not be changed (xls, xlsx, csv, txt, csvw)
type DownloadList struct {
	XLS  *DownloadObject `bson:"xls,omitempty" json:"xls,omitempty"`
	XLSX *DownloadObject `bson:"xlsx,omitempty" json:"xlsx,omitempty"`
	CSV  *DownloadObject `bson:"csv,omitempty" json:"csv,omitempty"`
	TXT  *DownloadObject `bson:"txt,omitempty" json:"txt,omitempty"`
	CSVW *DownloadObject `bson:"csvw,omitempty" json:"csvw,omitempty"`
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

func (vl *VersionLinks) DeepCopy() *VersionLinks {
	dst := &VersionLinks{}
	if vl.Dataset != nil {
		dst.Dataset = &LinkObject{
			ID:   vl.Dataset.ID,
			HRef: vl.Dataset.HRef,
		}
	}
	if vl.Dimensions != nil {
		dst.Dimensions = &LinkObject{
			ID:   vl.Dimensions.ID,
			HRef: vl.Dimensions.HRef,
		}
	}
	if vl.Edition != nil {
		dst.Edition = &LinkObject{
			ID:   vl.Edition.ID,
			HRef: vl.Edition.HRef,
		}
	}
	if vl.Self != nil {
		dst.Self = &LinkObject{
			ID:   vl.Self.ID,
			HRef: vl.Self.HRef,
		}
	}
	if vl.Spatial != nil {
		dst.Spatial = &LinkObject{
			ID:   vl.Spatial.ID,
			HRef: vl.Spatial.HRef,
		}
	}
	if vl.Version != nil {
		dst.Version = &LinkObject{
			ID:   vl.Version.ID,
			HRef: vl.Version.HRef,
		}
	}
	return dst
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

// CreateVersion manages the creation of a version from a reader
func CreateVersion(reader io.Reader, datasetID string) (*Version, error) {
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	log.Info(context.Background(), "DEBUG", log.Data{"body_create_version": string(b)})
	var version Version
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	version.ID = id.String()
	version.DatasetID = datasetID

	err = json.Unmarshal(b, &version)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
	}

	log.Info(context.Background(), "DEBUG", log.Data{"unmarshaled": version})

	return &version, nil
}

// CreateDownloadList manages the creation of a list downloadable items from a reader
func CreateDownloadList(reader io.Reader) (*DownloadList, error) {
	b, err := io.ReadAll(reader)
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
func CreateContact(reader io.Reader) (*ContactDetails, error) {
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}
	var contact ContactDetails
	err = json.Unmarshal(b, &contact)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

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

// ValidateVersion checks the content of the version structure
// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
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

		if version.Downloads.XLSX != nil {
			if version.Downloads.XLSX.HRef == "" {
				missingFields = append(missingFields, "Downloads.XLSX.HRef")
			}
			if version.Downloads.XLSX.Size == "" {
				missingFields = append(missingFields, "Downloads.XLSX.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.XLSX.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.XLSX.Size not a number")
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

		if version.Downloads.TXT != nil {
			if version.Downloads.TXT.HRef == "" {
				missingFields = append(missingFields, "Downloads.TXT.HRef")
			}
			if version.Downloads.TXT.Size == "" {
				missingFields = append(missingFields, "Downloads.TXT.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.TXT.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.TXT.Size not a number")
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

// ParseAndValidateVersionNumber checks the version is a positive integer above 0
func ParseAndValidateVersionNumber(ctx context.Context, version string) (int, error) {
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
