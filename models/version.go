package models

import (
	"context"
	//nolint:gosec //not used for secure purposes
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"go.mongodb.org/mongo-driver/bson"
)

// List of error variables
var (
	ErrAssociatedVersionCollectionIDInvalid = errors.New("missing collection_id for association between version and a collection")
	ErrPublishedVersionCollectionIDInvalid  = errors.New("unexpected collection_id in published version")
	ErrVersionStateInvalid                  = errors.New("incorrect state, can be one of the following: edition-confirmed, associated or published")
)

// Version represents information related to a single version for an edition of a dataset
type Version struct {
	Alerts             *[]Alert             `bson:"alerts,omitempty"                json:"alerts,omitempty"`
	CollectionID       string               `bson:"collection_id,omitempty"         json:"collection_id,omitempty"`
	DatasetID          string               `bson:"-"                               json:"dataset_id,omitempty"`
	Dimensions         []Dimension          `bson:"dimensions,omitempty"            json:"dimensions,omitempty"`
	Downloads          *DownloadList        `bson:"downloads,omitempty"             json:"downloads,omitempty"`
	Edition            string               `bson:"edition,omitempty"               json:"edition,omitempty"`
	EditionTitle       string               `bson:"edition_title,omitempty"         json:"edition_title,omitempty"`
	Headers            []string             `bson:"headers,omitempty"               json:"-"`
	ID                 string               `bson:"id,omitempty"                    json:"id,omitempty"`
	LastUpdated        time.Time            `bson:"last_updated,omitempty"          json:"last_updated,omitempty"`
	LatestChanges      *[]LatestChange      `bson:"latest_changes,omitempty"        json:"latest_changes,omitempty"`
	Links              *VersionLinks        `bson:"links,omitempty"                 json:"links,omitempty"`
	ReleaseDate        string               `bson:"release_date,omitempty"          json:"release_date,omitempty"`
	State              string               `bson:"state,omitempty"                 json:"state,omitempty"`
	Temporal           *[]TemporalFrequency `bson:"temporal,omitempty"              json:"temporal,omitempty"`
	UsageNotes         *[]UsageNote         `bson:"usage_notes,omitempty"           json:"usage_notes,omitempty"`
	IsBasedOn          *IsBasedOn           `bson:"is_based_on,omitempty"           json:"is_based_on,omitempty"`
	Version            int                  `bson:"version,omitempty"               json:"version,omitempty"`
	Type               string               `bson:"type,omitempty"                  json:"type,omitempty"`
	ETag               string               `bson:"e_tag"                           json:"-"`
	LowestGeography    string               `bson:"lowest_geography,omitempty"      json:"lowest_geography,omitempty"`
	QualityDesignation QualityDesignation   `bson:"quality_designation,omitempty"   json:"quality_designation,omitempty"`
	Distributions      *[]Distribution      `bson:"distributions,omitempty"         json:"distributions,omitempty"`
}

// Alert represents an object containing information on an alert
type Alert struct {
	Date        string    `bson:"date,omitempty"        json:"date,omitempty"`
	Description string    `bson:"description,omitempty" json:"description,omitempty"`
	Type        AlertType `bson:"type,omitempty"        json:"type,omitempty"`
}

// Distribution represents a specific distribution of the dataset
type Distribution struct {
	Title       string                `bson:"title,omitempty"         json:"title,omitempty"`
	Format      DistributionFormat    `bson:"format,omitempty"        json:"format,omitempty"`
	MediaType   DistributionMediaType `bson:"media_type,omitempty"    json:"media_type,omitempty"`
	DownloadURL string                `bson:"download_url,omitempty"  json:"download_url,omitempty"`
	ByteSize    int64                 `bson:"byte_size,omitempty"     json:"byte_size,omitempty"`
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

// Maps `DownloadObjects` in input `DownloadList` to their corresponding extension strings
func (dl *DownloadList) ExtensionsMapping() map[*DownloadObject]string {
	return map[*DownloadObject]string{
		dl.CSV:  "csv",
		dl.CSVW: "csvw",
		dl.TXT:  "txt",
		dl.XLS:  "xls",
		dl.XLSX: "xlsx",
	}
}

// LatestChange represents an object containing
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

// AlertType defines possible types of alerts
type AlertType string

// Define the possible values for the AlertType enum
const (
	AlertTypeAlert      AlertType = "alert"
	AlertTypeCorrection AlertType = "correction"
)

// IsValid validates that the AlertType is a valid enum value
func (at AlertType) IsValid() bool {
	switch at {
	case AlertTypeAlert, AlertTypeCorrection:
		return true
	default:
		return false
	}
}

// String returns the string value of the AlertType
func (at AlertType) String() string {
	return string(at)
}

// MarshalJSON marshals the AlertType to JSON
func (at AlertType) MarshalJSON() ([]byte, error) {
	if !at.IsValid() {
		return nil, fmt.Errorf("invalid AlertType: %s", at)
	}
	return json.Marshal(string(at))
}

// UnmarshalJSON unmarshals a string to AlertType
func (at *AlertType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	converted := AlertType(str)
	if !converted.IsValid() {
		return fmt.Errorf("invalid AlertType: %s", str)
	}
	*at = converted
	return nil
}

// QualityDesignation enum type representing the allowable quality designations for a dataset
type QualityDesignation string

// Define the possible values for the QualityDesignation enum
const (
	QualityDesignationAccreditedOfficial    QualityDesignation = "accredited-official"
	QualityDesignationOfficialInDevelopment QualityDesignation = "official-in-development"
	QualityDesignationOfficial              QualityDesignation = "official"
)

// IsValid validates that the QualityDesignation is a valid enum value
func (qd *QualityDesignation) IsValid() bool {
	switch *qd {
	case QualityDesignationAccreditedOfficial, QualityDesignationOfficialInDevelopment, QualityDesignationOfficial:
		return true
	default:
		return false
	}
}

// String returns the string value of the QualityDesignation
func (qd QualityDesignation) String() string {
	return string(qd)
}

// MarshalJSON marshals the QualityDesignation to JSON
func (qd QualityDesignation) MarshalJSON() ([]byte, error) {
	if !qd.IsValid() {
		return nil, fmt.Errorf("invalid QualityDesignation: %s", qd)
	}
	return json.Marshal(string(qd))
}

// UnmarshalJSON unmarshals a string to QualityDesignation
func (qd *QualityDesignation) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	converted := QualityDesignation(str)
	if !converted.IsValid() {
		return fmt.Errorf("invalid QualityDesignation: %s", str)
	}
	*qd = converted
	return nil
}

// DistributionFormat enum type representing the allowable formats for the distribution download file
type DistributionFormat string

// Define the possible values for the DistributionFormat enum
const (
	DistributionFormatCSV      DistributionFormat = "csv"
	DistributionFormatSDMX     DistributionFormat = "sdmx"
	DistributionFormatXLS      DistributionFormat = "xls"
	DistributionFormatXLSX     DistributionFormat = "xlsx"
	DistributionFormatCSDB     DistributionFormat = "csdb"
	DistributionFormatCSVWMeta DistributionFormat = "csvw-metadata"
)

// IsValid validates that the DistributionFormat is a valid enum value
func (f *DistributionFormat) IsValid() bool {
	switch *f {
	case DistributionFormatCSV, DistributionFormatSDMX, DistributionFormatXLS,
		DistributionFormatXLSX, DistributionFormatCSDB, DistributionFormatCSVWMeta:
		return true
	default:
		return false
	}
}

// String returns the string value of the DistributionFormat
func (f DistributionFormat) String() string {
	return string(f)
}

// MarshalJSON marshals the DistributionFormat to JSON
func (f DistributionFormat) MarshalJSON() ([]byte, error) {
	if !f.IsValid() {
		return nil, fmt.Errorf("invalid DistributionFormat: %s", f)
	}
	return json.Marshal(string(f))
}

// UnmarshalJSON unmarshals a string to DistributionFormat
func (f *DistributionFormat) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	converted := DistributionFormat(str)
	if !converted.IsValid() {
		return fmt.Errorf("invalid DistributionFormat: %s", str)
	}
	*f = converted
	return nil
}

// DistributionMediaType enum type representing the allowable media types for the distribution download file
type DistributionMediaType string

// Define the possible values for the DistributionMediaType enum
const (
	DistributionMediaTypeCSV      DistributionMediaType = "text/csv"
	DistributionMediaTypeSDMX     DistributionMediaType = "application/vnd.sdmx.structurespecificdata+xml"
	DistributionMediaTypeXLS      DistributionMediaType = "application/vnd.ms-excel"
	DistributionMediaTypeXLSX     DistributionMediaType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	DistributionMediaTypeCSDB     DistributionMediaType = "text/plain"
	DistributionMediaTypeCSVWMeta DistributionMediaType = "application/ld+json"
)

// IsValid validates that the DistributionMediaType is a valid enum value
func (mt *DistributionMediaType) IsValid() bool {
	switch *mt {
	case DistributionMediaTypeCSV, DistributionMediaTypeSDMX, DistributionMediaTypeXLS,
		DistributionMediaTypeXLSX, DistributionMediaTypeCSDB, DistributionMediaTypeCSVWMeta:
		return true
	default:
		return false
	}
}

// String returns the string value of the DistributionMediaType
func (mt DistributionMediaType) String() string {
	return string(mt)
}

// MarshalJSON marshals the DistributionMediaType to JSON
func (mt DistributionMediaType) MarshalJSON() ([]byte, error) {
	if !mt.IsValid() {
		return nil, fmt.Errorf("invalid DistributionMediaType: %s", mt)
	}
	return json.Marshal(string(mt))
}

// UnmarshalJSON unmarshals a string to DistributionMediaType
func (mt *DistributionMediaType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	converted := DistributionMediaType(str)
	if !converted.IsValid() {
		return fmt.Errorf("invalid DistributionMediaType: %s", str)
	}
	*mt = converted
	return nil
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
