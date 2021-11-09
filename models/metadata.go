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
	Dimensions        []Dimension          `json:"dimensions,omitempty"`
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
	Coverage          string               `json:"coverage,omitempty"`
	TablePopulation   string               `json:"table_population,omitempty"`
	AreaType          string               `json:"area_type,omitempty"`
	TableID           string               `json:"table_id,omitempty"`
	Classifications   string               `json:"classifications,omitempty"`
	Source            string               `json:"source,omitempty"`
	CSVHeader         []string             `json:"headers,omitempty"`
}

// MetadataLinks represents a link object to list of metadata relevant to a version
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

	// Remove Public and Private download links
	if metaDataDoc.Downloads != nil {
		if metaDataDoc.Downloads.CSV != nil {
			metaDataDoc.Downloads.CSV.Private = ""
			metaDataDoc.Downloads.CSV.Public = ""
		}
		if metaDataDoc.Downloads.CSVW != nil {
			metaDataDoc.Downloads.CSVW.Private = ""
			metaDataDoc.Downloads.CSVW.Public = ""
		}
		if metaDataDoc.Downloads.XLS != nil {
			metaDataDoc.Downloads.XLS.Private = ""
			metaDataDoc.Downloads.XLS.Public = ""
		}
	}

	return metaDataDoc
}

// CreateCantabularMetaDataDoc manages the creation of metadata across dataset and version docs for cantabular datasets
// note: logic to retrieve the newly-added Cantabular-specific fields to the Metadata model will be created at a later date
func CreateCantabularMetaDataDoc(d *Dataset, v *Version, urlBuilder *url.Builder) *Metadata {
	m := &Metadata{
		CSVHeader:     v.Headers,
		Description:   d.Description,
		Dimensions:    v.Dimensions,
		Downloads:     v.Downloads,
		Keywords:      d.Keywords,
		ReleaseDate:   v.ReleaseDate,
		Title:         d.Title,
		UnitOfMeasure: d.UnitOfMeasure,
	}

	m.Distribution = getDistribution(m.Downloads)

	// Remove Public and Private download links
	if m.Downloads != nil {
		if m.Downloads.CSV != nil {
			m.Downloads.CSV.Private = ""
			m.Downloads.CSV.Public = ""
		}
		if m.Downloads.CSVW != nil {
			m.Downloads.CSVW.Private = ""
			m.Downloads.CSVW.Public = ""
		}
		if m.Downloads.XLS != nil {
			m.Downloads.XLS.Private = ""
			m.Downloads.XLS.Public = ""
		}
		if m.Downloads.TXT != nil {
			m.Downloads.TXT.Private = ""
			m.Downloads.TXT.Public = ""
		}
	}

	return m
}

func getDistribution(downloads *DownloadList) []string {
	distribution := []string{"json"}

	if downloads != nil {
		if downloads.CSV != nil && downloads.CSV.HRef != "" {
			distribution = append(distribution, "csv")
		}

		if downloads.CSVW != nil && downloads.CSVW.HRef != "" {
			distribution = append(distribution, "csvw")
		}

		if downloads.XLS != nil && downloads.XLS.HRef != "" {
			distribution = append(distribution, "xls")
		}

		if downloads.TXT != nil && downloads.TXT.HRef != "" {
			distribution = append(distribution, "txt")
		}
	}

	return distribution
}
