package models

import (
	"strconv"

	"github.com/ONSdigital/dp-dataset-api/url"
)

// Metadata represents information (metadata) relevant to a version
type Metadata struct {
	EditableMetadata
	Distribution    []string             `json:"distribution,omitempty"`
	Downloads       *DownloadList        `json:"downloads,omitempty"`
	Links           *MetadataLinks       `json:"links,omitempty"`
	TableID         string               `json:"table_id,omitempty"`
	CSVHeader       []string             `json:"headers,omitempty"`
	Version         int                  `json:"version,omitempty"`
	DatasetLinks    *DatasetLinks        `json:"dataset_links,omitempty"`
	Publisher       *Publisher           `json:"publisher,omitempty"`
	Temporal        *[]TemporalFrequency `json:"temporal,omitempty"`
	Theme           string               `json:"theme,omitempty"`
	URI             string               `json:"uri,omitempty"`
	Coverage        string               `json:"coverage,omitempty"`
	TablePopulation string               `json:"table_population,omitempty"`
	AreaType        string               `json:"area_type,omitempty"`
	Classifications string               `json:"classifications,omitempty"`
	Source          string               `json:"source,omitempty"`
	IsBasedOn       *IsBasedOn           `json:"is_based_on,omitempty"`
}

// EditableMetadata represents the metadata fields that can be edited
type EditableMetadata struct {
	Alerts            *[]Alert         `json:"alerts,omitempty"`
	CanonicalTopic    string           `json:"canonical_topic,omitempty"`
	Contacts          []ContactDetails `json:"contacts,omitempty"`
	Description       string           `json:"description,omitempty"`
	Dimensions        []Dimension      `json:"dimensions,omitempty"`
	Keywords          []string         `json:"keywords,omitempty"`
	LatestChanges     *[]LatestChange  `json:"latest_changes,omitempty"`
	License           string           `json:"license,omitempty"`
	Methodologies     []GeneralDetails `json:"methodologies,omitempty"`
	NationalStatistic *bool            `json:"national_statistic,omitempty"`
	NextRelease       string           `json:"next_release,omitempty"`
	Publications      []GeneralDetails `json:"publications,omitempty"`
	QMI               *GeneralDetails  `json:"qmi,omitempty"`
	RelatedDatasets   []GeneralDetails `json:"related_datasets,omitempty"`
	ReleaseDate       string           `json:"release_date,omitempty"`
	ReleaseFrequency  string           `json:"release_frequency,omitempty"`
	Title             string           `json:"title,omitempty"`
	Survey            string           `json:"survey,omitempty"`
	Subtopics         []string         `json:"subtopics,omitempty"`
	UnitOfMeasure     string           `json:"unit_of_measure,omitempty"`
	UsageNotes        *[]UsageNote     `json:"usage_notes,omitempty"`
	RelatedContent    []GeneralDetails `json:"related_content,omitempty"`
	Themes            []string         `json:"themes,omitempty"`
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
		EditableMetadata: EditableMetadata{
			Alerts:            versionDoc.Alerts,
			CanonicalTopic:    datasetDoc.CanonicalTopic,
			Contacts:          datasetDoc.Contacts,
			Description:       datasetDoc.Description,
			Dimensions:        versionDoc.Dimensions,
			Keywords:          datasetDoc.Keywords,
			LatestChanges:     versionDoc.LatestChanges,
			License:           datasetDoc.License,
			Methodologies:     datasetDoc.Methodologies,
			NationalStatistic: datasetDoc.NationalStatistic,
			NextRelease:       datasetDoc.NextRelease,
			Publications:      datasetDoc.Publications,
			QMI:               datasetDoc.QMI,
			RelatedDatasets:   datasetDoc.RelatedDatasets,
			ReleaseDate:       versionDoc.ReleaseDate,
			ReleaseFrequency:  datasetDoc.ReleaseFrequency,
			Subtopics:         datasetDoc.Subtopics,
			Title:             datasetDoc.Title,
			UnitOfMeasure:     datasetDoc.UnitOfMeasure,
			UsageNotes:        versionDoc.UsageNotes,
		},
		Links:     &MetadataLinks{},
		Publisher: datasetDoc.Publisher,
		Temporal:  versionDoc.Temporal,
		Theme:     datasetDoc.Theme,
		URI:       datasetDoc.URI,
		IsBasedOn: datasetDoc.IsBasedOn,
	}

	// Add relevant metdata links from dataset document
	if datasetDoc.Links != nil {
		metaDataDoc.Links.AccessRights = datasetDoc.Links.AccessRights
	}

	if metaDataDoc.Themes == nil {
		if datasetDoc.CanonicalTopic != "" {
			metaDataDoc.Themes = append(metaDataDoc.Themes, datasetDoc.CanonicalTopic)
		}

		if datasetDoc.Subtopics != nil {
			metaDataDoc.Themes = append(metaDataDoc.Themes, datasetDoc.Subtopics...)
		}
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

	metaDataDoc.Distribution = getDistribution(versionDoc.Downloads)

	if versionDoc.Downloads != nil {
		metaDataDoc.Downloads = &DownloadList{}
		if versionDoc.Downloads.CSV != nil {
			metaDataDoc.Downloads.CSV = &DownloadObject{
				HRef: versionDoc.Downloads.CSV.HRef,
				Size: versionDoc.Downloads.CSV.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if versionDoc.Downloads.CSVW != nil {
			metaDataDoc.Downloads.CSVW = &DownloadObject{
				HRef: versionDoc.Downloads.CSVW.HRef,
				Size: versionDoc.Downloads.CSVW.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if versionDoc.Downloads.XLS != nil {
			metaDataDoc.Downloads.XLS = &DownloadObject{
				HRef: versionDoc.Downloads.XLS.HRef,
				Size: versionDoc.Downloads.XLS.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if versionDoc.Downloads.TXT != nil {
			metaDataDoc.Downloads.TXT = &DownloadObject{
				HRef:    versionDoc.Downloads.TXT.HRef,
				Size:    versionDoc.Downloads.TXT.Size,
				Private: versionDoc.Downloads.TXT.Private,
				Public:  versionDoc.Downloads.TXT.Public,
			}
		}
		if versionDoc.Downloads.XLSX != nil {
			metaDataDoc.Downloads.XLSX = &DownloadObject{
				HRef:    versionDoc.Downloads.XLSX.HRef,
				Size:    versionDoc.Downloads.XLSX.Size,
				Private: versionDoc.Downloads.XLSX.Private,
				Public:  versionDoc.Downloads.XLSX.Public,
			}
		}
	}

	return metaDataDoc
}

// CreateCantabularMetaDataDoc manages the creation of metadata across dataset and version docs for cantabular datasets
// note: logic to retrieve the newly-added Cantabular-specific fields to the Metadata model will be created at a later date
func CreateCantabularMetaDataDoc(d *Dataset, v *Version) *Metadata {
	m := &Metadata{
		EditableMetadata: EditableMetadata{
			CanonicalTopic: d.CanonicalTopic,
			Contacts:       d.Contacts,
			Description:    d.Description,
			Dimensions:     v.Dimensions,
			Keywords:       d.Keywords,
			RelatedContent: d.RelatedContent,
			ReleaseDate:    v.ReleaseDate,
			Subtopics:      d.Subtopics,
			Title:          d.Title,
			UnitOfMeasure:  d.UnitOfMeasure,
			QMI:            d.QMI,
		},
		CSVHeader:    v.Headers,
		DatasetLinks: d.Links,
		Version:      v.Version,
		URI:          d.URI,
		IsBasedOn:    d.IsBasedOn,
	}

	m.Distribution = getDistribution(v.Downloads)

	if v.Downloads != nil {
		m.Downloads = &DownloadList{}
		if v.Downloads.CSV != nil {
			m.Downloads.CSV = &DownloadObject{
				HRef: v.Downloads.CSV.HRef,
				Size: v.Downloads.CSV.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if v.Downloads.CSVW != nil {
			m.Downloads.CSVW = &DownloadObject{
				HRef: v.Downloads.CSVW.HRef,
				Size: v.Downloads.CSVW.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if v.Downloads.XLS != nil {
			m.Downloads.XLS = &DownloadObject{
				HRef: v.Downloads.XLS.HRef,
				Size: v.Downloads.XLS.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if v.Downloads.TXT != nil {
			m.Downloads.TXT = &DownloadObject{
				HRef: v.Downloads.TXT.HRef,
				Size: v.Downloads.TXT.Size,
				// Do not include Public and Private download links
				Public:  "",
				Private: "",
			}
		}
		if v.Downloads.XLSX != nil {
			m.Downloads.XLSX = &DownloadObject{
				HRef:    v.Downloads.XLSX.HRef,
				Size:    v.Downloads.XLSX.Size,
				Private: v.Downloads.XLSX.Private,
				Public:  v.Downloads.XLSX.Public,
			}
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

// UpdateMetadata updates the metadata fields for a dataset
func (d *Dataset) UpdateMetadata(metadata EditableMetadata) {
	d.CanonicalTopic = metadata.CanonicalTopic
	d.Title = metadata.Title
	d.Contacts = metadata.Contacts
	d.NextRelease = metadata.NextRelease
	d.License = metadata.License
	d.Description = metadata.Description
	d.UnitOfMeasure = metadata.UnitOfMeasure
	d.Keywords = metadata.Keywords
	d.Subtopics = metadata.Subtopics
	d.RelatedContent = metadata.RelatedContent
	d.NationalStatistic = metadata.NationalStatistic
	d.Methodologies = metadata.Methodologies
	d.QMI = metadata.QMI
	d.ReleaseFrequency = metadata.ReleaseFrequency
	d.RelatedDatasets = metadata.RelatedDatasets
	d.Publications = metadata.Publications
	d.Survey = metadata.Survey
	if metadata.CanonicalTopic != "" {
		if !contains(d.Themes, metadata.CanonicalTopic) {
			d.Themes = append(d.Themes, metadata.CanonicalTopic)
		}
	}

	if metadata.Subtopics != nil {
		for _, subtopic := range metadata.Subtopics {
			if !contains(d.Themes, subtopic) {
				d.Themes = append(d.Themes, subtopic)
			}
		}
	}
}

// UpdateMetadata updates the metadata fields for a version
func (v *Version) UpdateMetadata(metadata EditableMetadata) {
	v.ReleaseDate = metadata.ReleaseDate
	v.Alerts = metadata.Alerts
	v.Dimensions = metadata.Dimensions
	v.UsageNotes = metadata.UsageNotes
	v.LatestChanges = metadata.LatestChanges
}

func contains(themes []string, item string) bool {
	for _, v := range themes {
		if v == item {
			return true
		}
	}
	return false
}
