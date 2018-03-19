package models

import (
	"strconv"

	"github.com/ONSdigital/dp-dataset-api/url"
)

// Metadata represents information (metadata) relevant to a version
type Metadata struct {
	Alerts            *[]Alert             `json:"alerts,omitempty"`
	Contacts          []ContactDetails     `json:"contacts,omitempty"`
	Description       string               `json:"description,omitempty"`
	Dimensions        []CodeList           `json:"dimensions,omitempty"`
	Distribution      []string             `json:"distribution,omitempty"`
	Downloads         *DownloadList        `json:"downloads,omitempty"`
	Keywords          []string             `json:"keywords,omitempty"`
	LatestChanges     *[]LatestChange      `json:"latest_changes,omitempty"`
	License           string               `json:"license,omitempty"`
	Links             *MetadataLinks       `json:"links,omitempty"`
	Methodologies     []GeneralDetails     `json:"methodologies,omitempty"`
	NationalStatistic *bool                `json:"national_statistic,omitempty"`
	NextRelease       string               `json:"next_release,omitempty"`
	Publications      []GeneralDetails     `json:"publications,omitempty"`
	Publisher         *Publisher           `json:"publisher,omitempty"`
	QMI               *GeneralDetails      `json:"qmi,omitempty"`
	RelatedDatasets   []GeneralDetails     `json:"related_datasets,omitempty"`
	ReleaseDate       string               `json:"release_date,omitempty"`
	ReleaseFrequency  string               `json:"release_frequency,omitempty"`
	Temporal          *[]TemporalFrequency `json:"temporal,omitempty"`
	Theme             string               `json:"theme,omitempty"`
	Title             string               `json:"title,omitempty"`
	UnitOfMeasure     string               `json:"unit_of_measure,omitempty"`
	URI               string               `json:"uri,omitempty"`
	UsageNotes        *[]UsageNote         `json:"usage_notes,omitempty"`
}

// MetadataLinks represents a link object to list of metadata) relevant to a version
type MetadataLinks struct {
	AccessRights   *LinkObject `json:"access_rights,omitempty"`
	Self           *LinkObject `json:"self,omitempty"`
	Spatial        *LinkObject `json:"spatial,omitempty"`
	Version        *LinkObject `json:"version,omitempty"`
	WebsiteVersion *LinkObject `json:"website_version,omitempty"`
}

// CreateMetaDataDoc manages the creation of metadata across dataset and version docs
func CreateMetaDataDoc(datasetDoc *Dataset, versionDoc *Version, urlBuilder *url.Builder) *Metadata {
	metaDataDoc := &Metadata{
		Alerts:            versionDoc.Alerts,
		Contacts:          datasetDoc.Contacts,
		Description:       datasetDoc.Description,
		Dimensions:        versionDoc.Dimensions,
		Downloads:         versionDoc.Downloads,
		Keywords:          datasetDoc.Keywords,
		LatestChanges:     versionDoc.LatestChanges,
		Links:             &MetadataLinks{},
		License:           datasetDoc.License,
		Methodologies:     datasetDoc.Methodologies,
		NationalStatistic: datasetDoc.NationalStatistic,
		NextRelease:       datasetDoc.NextRelease,
		Publications:      datasetDoc.Publications,
		Publisher:         datasetDoc.Publisher,
		QMI:               datasetDoc.QMI,
		RelatedDatasets:   datasetDoc.RelatedDatasets,
		ReleaseDate:       versionDoc.ReleaseDate,
		ReleaseFrequency:  datasetDoc.ReleaseFrequency,
		Temporal:          versionDoc.Temporal,
		Theme:             datasetDoc.Theme,
		Title:             datasetDoc.Title,
		UnitOfMeasure:     datasetDoc.UnitOfMeasure,
		URI:               datasetDoc.URI,
		UsageNotes:        versionDoc.UsageNotes,
	}

	// Add relevant metdata links from dataset document
	if datasetDoc.Links != nil {
		metaDataDoc.Links.AccessRights = datasetDoc.Links.AccessRights
	}

	// Add relevant metdata links from version document
	if versionDoc.Links != nil {

		if versionDoc.Links.Version != nil && versionDoc.Links.Version.HRef != "" {
			metaDataDoc.Links.Self = &LinkObject{
				HRef: versionDoc.Links.Version.HRef + "/metadata",
			}
		}

		metaDataDoc.Links.Spatial = versionDoc.Links.Spatial
		metaDataDoc.Links.Version = versionDoc.Links.Version

		websiteVersionURL := urlBuilder.BuildWebsiteDatasetVersionURL(
			datasetDoc.ID,
			versionDoc.Links.Edition.ID,
			strconv.Itoa(versionDoc.Version))

		metaDataDoc.Links.WebsiteVersion = &LinkObject{
			HRef: websiteVersionURL,
		}
	}

	metaDataDoc.Distribution = getDistribution(metaDataDoc.Downloads)

	return metaDataDoc
}

func getDistribution(downloads *DownloadList) []string {
	distribution := []string{"json"}

	if downloads != nil {
		if downloads.CSV != nil && downloads.CSV.URL != "" {
			distribution = append(distribution, "csv")
		}

		if downloads.XLS != nil && downloads.XLS.URL != "" {
			distribution = append(distribution, "xls")
		}
	}

	return distribution
}
