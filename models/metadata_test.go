package models

import (
	"fmt"
	neturl "net/url"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/smartystreets/goconvey/convey"
)

func TestCreateMetadata(t *testing.T) {
	convey.Convey("Given a dataset and a version objects", t, func() {
		dataset := Dataset{
			ID:             "9875",
			CanonicalTopic: "1234",
			Subtopics:      []string{"5678", "9012"},
			Description:    "census",
			Keywords:       []string{"test", "test2"},
			Title:          "CensusEthnicity",
			UnitOfMeasure:  "Pounds Sterling",
			Contacts:       []ContactDetails{contacts},
			URI:            "http://localhost:22000/datasets/123/breadcrumbs",
			QMI:            &qmi,
			RelatedContent: []GeneralDetails{relatedDatasets},
			License:        "license",
			Methodologies: []GeneralDetails{
				methodology,
			},
			NationalStatistic: &nationalStatistic,
			NextRelease:       "2023-03-14",
			Publications: []GeneralDetails{
				publications,
			},
			Publisher: &publisher,
			RelatedDatasets: []GeneralDetails{
				relatedDatasets,
			},
			ReleaseFrequency: "yearly",
			Theme:            "population",
			IsBasedOn: &IsBasedOn{
				ID:   "UR_HH",
				Type: "All usual residents in households",
			},
			Links: &DatasetLinks{
				AccessRights: &LinkObject{
					HRef: "href-access-rights",
					ID:   "access-rights",
				},
				Editions: &LinkObject{
					HRef: "href-editions",
					ID:   "editions",
				},
				LatestVersion: &LinkObject{
					HRef: "href-latest",
					ID:   "latest",
				},
				Self: &LinkObject{
					HRef: "href-self",
					ID:   "self",
				},
				Taxonomy: &LinkObject{
					HRef: "href-taxonomy",
					ID:   "taxonomy",
				},
			},
		}

		csvDownload := DownloadObject{
			HRef:    "https://www.aws/123csv",
			Private: "csv-private",
			Public:  "csv-public",
			Size:    "252",
		}
		csvwDownload := DownloadObject{
			HRef:    "https://www.aws/123",
			Private: "csvw-private",
			Public:  "csvw-public",
			Size:    "25",
		}
		xlsDownload := DownloadObject{
			HRef:    "https://www.aws/1234",
			Private: "xls-private",
			Public:  "xls-public",
			Size:    "45",
		}
		txtDownload := DownloadObject{
			HRef:    "https://www.aws/txt",
			Private: "txt-private",
			Public:  "txt-public",
			Size:    "11",
		}
		xlsxDownload := DownloadObject{
			HRef:    "https://www.aws/xlsx",
			Private: "xlsx-private",
			Public:  "xlsx-public",
			Size:    "119",
		}

		version := Version{
			Alerts:     &[]Alert{alert},
			Dimensions: []Dimension{dimension},
			Downloads: &DownloadList{
				CSV:  &csvDownload,
				CSVW: &csvwDownload,
				XLS:  &xlsDownload,
				TXT:  &txtDownload,
				XLSX: &xlsxDownload,
			},
			Edition:       "2017",
			Headers:       []string{"cantabular_table", "age"},
			LatestChanges: &[]LatestChange{latestChange},
			Links:         &links,
			ReleaseDate:   "2017-10-12",
			State:         PublishedState,
			Temporal:      &[]TemporalFrequency{temporal},
			Version:       1,
		}

		convey.Convey("When we call CreateCantabularMetaDataDoc", func() {
			metaDataDoc := CreateCantabularMetaDataDoc(&dataset, &version)

			convey.Convey("Then it returns a metadata object with all the Cantabular fields populated", func() {
				convey.So(metaDataDoc.Description, convey.ShouldEqual, dataset.Description)
				convey.So(metaDataDoc.Keywords, convey.ShouldResemble, dataset.Keywords)
				convey.So(metaDataDoc.Title, convey.ShouldEqual, dataset.Title)
				convey.So(metaDataDoc.UnitOfMeasure, convey.ShouldEqual, dataset.UnitOfMeasure)
				convey.So(metaDataDoc.Contacts, convey.ShouldResemble, dataset.Contacts)
				convey.So(metaDataDoc.URI, convey.ShouldEqual, dataset.URI)
				convey.So(metaDataDoc.QMI, convey.ShouldResemble, dataset.QMI)
				convey.So(metaDataDoc.DatasetLinks, convey.ShouldResemble, dataset.Links)
				convey.So(metaDataDoc.RelatedContent, convey.ShouldResemble, dataset.RelatedContent)
				convey.So(metaDataDoc.CanonicalTopic, convey.ShouldEqual, dataset.CanonicalTopic)
				convey.So(metaDataDoc.Subtopics, convey.ShouldResemble, dataset.Subtopics)

				convey.So(metaDataDoc.CSVHeader, convey.ShouldResemble, version.Headers)
				convey.So(metaDataDoc.Dimensions, convey.ShouldResemble, version.Dimensions)
				convey.So(metaDataDoc.ReleaseDate, convey.ShouldEqual, version.ReleaseDate)
				convey.So(metaDataDoc.Version, convey.ShouldEqual, version.Version)

				convey.So(metaDataDoc.Downloads.CSV.HRef, convey.ShouldEqual, csvDownload.HRef)
				convey.So(metaDataDoc.Downloads.CSV.Size, convey.ShouldEqual, csvDownload.Size)
				convey.So(metaDataDoc.Downloads.CSV.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.CSV.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.CSVW.HRef, convey.ShouldEqual, csvwDownload.HRef)
				convey.So(metaDataDoc.Downloads.CSVW.Size, convey.ShouldEqual, csvwDownload.Size)
				convey.So(metaDataDoc.Downloads.CSVW.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.CSVW.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.TXT.HRef, convey.ShouldEqual, txtDownload.HRef)
				convey.So(metaDataDoc.Downloads.TXT.Size, convey.ShouldEqual, txtDownload.Size)
				convey.So(metaDataDoc.Downloads.TXT.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.TXT.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.XLS.HRef, convey.ShouldEqual, xlsDownload.HRef)
				convey.So(metaDataDoc.Downloads.XLS.Size, convey.ShouldEqual, xlsDownload.Size)
				convey.So(metaDataDoc.Downloads.XLS.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.XLS.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.XLSX.HRef, convey.ShouldEqual, xlsxDownload.HRef)
				convey.So(metaDataDoc.Downloads.XLSX.Size, convey.ShouldEqual, xlsxDownload.Size)
				convey.So(metaDataDoc.Downloads.XLSX.Private, convey.ShouldEqual, xlsxDownload.Private) // TODO: Should it be cleared?
				convey.So(metaDataDoc.Downloads.XLSX.Public, convey.ShouldEqual, xlsxDownload.Public)   // TODO: Should it be cleared?
				convey.So(metaDataDoc.IsBasedOn, convey.ShouldResemble, &IsBasedOn{
					ID:   "UR_HH",
					Type: "All usual residents in households",
				})
				convey.So(metaDataDoc.Version, convey.ShouldEqual, 1)

				// TODO: convey.Should it include xlsx?
				convey.So(metaDataDoc.Distribution, convey.ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})
			})

			convey.Convey("And the non-Cantabular fields are empty", func() {
				convey.So(metaDataDoc.Alerts, convey.ShouldBeNil)
				convey.So(metaDataDoc.LatestChanges, convey.ShouldBeNil)
				convey.So(metaDataDoc.Links, convey.ShouldBeNil)
				convey.So(metaDataDoc.License, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Methodologies, convey.ShouldBeNil)
				convey.So(metaDataDoc.NationalStatistic, convey.ShouldBeNil)
				convey.So(metaDataDoc.NextRelease, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Publications, convey.ShouldBeNil)
				convey.So(metaDataDoc.Publisher, convey.ShouldBeNil)
				convey.So(metaDataDoc.RelatedDatasets, convey.ShouldBeNil)
				convey.So(metaDataDoc.ReleaseFrequency, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Temporal, convey.ShouldBeNil)
				convey.So(metaDataDoc.Theme, convey.ShouldEqual, "")
				convey.So(metaDataDoc.UsageNotes, convey.ShouldBeNil)
			})
		})

		convey.Convey("When we call CreateMetaDataDoc", func() {
			codeListAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:22400"}
			datasetAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:22000"}
			downloadServiceURL := &neturl.URL{Scheme: "http", Host: "localhost:23600"}
			importAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:21800"}
			websiteURL := &neturl.URL{Scheme: "http", Host: "localhost:20000"}
			urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)
			metaDataDoc := CreateMetaDataDoc(&dataset, &version, urlBuilder)
			expectedThemes := []string{"1234", "5678", "9012"}

			convey.Convey("Then it returns a metadata object with all the CMD fields populated", func() {
				convey.So(metaDataDoc.Description, convey.ShouldEqual, dataset.Description)
				convey.So(metaDataDoc.Keywords, convey.ShouldResemble, dataset.Keywords)
				convey.So(metaDataDoc.Title, convey.ShouldEqual, dataset.Title)
				convey.So(metaDataDoc.UnitOfMeasure, convey.ShouldEqual, dataset.UnitOfMeasure)
				convey.So(metaDataDoc.Contacts, convey.ShouldResemble, dataset.Contacts)
				convey.So(metaDataDoc.License, convey.ShouldResemble, dataset.License)
				convey.So(metaDataDoc.Methodologies, convey.ShouldResemble, dataset.Methodologies)
				convey.So(metaDataDoc.NationalStatistic, convey.ShouldResemble, dataset.NationalStatistic)
				convey.So(metaDataDoc.NextRelease, convey.ShouldResemble, dataset.NextRelease)
				convey.So(metaDataDoc.Publications, convey.ShouldResemble, dataset.Publications)
				convey.So(metaDataDoc.Publisher, convey.ShouldResemble, dataset.Publisher)
				convey.So(metaDataDoc.RelatedDatasets, convey.ShouldResemble, dataset.RelatedDatasets)
				convey.So(metaDataDoc.ReleaseFrequency, convey.ShouldResemble, dataset.ReleaseFrequency)
				convey.So(metaDataDoc.Theme, convey.ShouldResemble, dataset.Theme)
				convey.So(metaDataDoc.URI, convey.ShouldEqual, dataset.URI)
				convey.So(metaDataDoc.QMI, convey.ShouldResemble, dataset.QMI)
				convey.So(metaDataDoc.CanonicalTopic, convey.ShouldEqual, dataset.CanonicalTopic)
				convey.So(metaDataDoc.Subtopics, convey.ShouldResemble, dataset.Subtopics)
				convey.So(metaDataDoc.Links.AccessRights, convey.ShouldEqual, dataset.Links.AccessRights)
				convey.So(metaDataDoc.Themes, convey.ShouldEqual, expectedThemes)

				convey.So(metaDataDoc.Alerts, convey.ShouldEqual, version.Alerts)
				convey.So(metaDataDoc.Dimensions, convey.ShouldResemble, version.Dimensions)
				convey.So(metaDataDoc.LatestChanges, convey.ShouldEqual, version.LatestChanges)
				convey.So(metaDataDoc.ReleaseDate, convey.ShouldEqual, version.ReleaseDate)
				convey.So(metaDataDoc.Temporal, convey.ShouldEqual, version.Temporal)
				convey.So(metaDataDoc.UsageNotes, convey.ShouldEqual, version.UsageNotes)

				convey.So(metaDataDoc.Downloads.CSV.HRef, convey.ShouldEqual, csvDownload.HRef)
				convey.So(metaDataDoc.Downloads.CSV.Size, convey.ShouldEqual, csvDownload.Size)
				convey.So(metaDataDoc.Downloads.CSV.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.CSV.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.CSVW.HRef, convey.ShouldEqual, csvwDownload.HRef)
				convey.So(metaDataDoc.Downloads.CSVW.Size, convey.ShouldEqual, csvwDownload.Size)
				convey.So(metaDataDoc.Downloads.CSVW.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.CSVW.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.TXT.HRef, convey.ShouldEqual, txtDownload.HRef)
				convey.So(metaDataDoc.Downloads.TXT.Size, convey.ShouldEqual, txtDownload.Size)
				convey.So(metaDataDoc.Downloads.TXT.Private, convey.ShouldEqual, txtDownload.Private) // TODO: Should it be cleared?
				convey.So(metaDataDoc.Downloads.TXT.Public, convey.ShouldEqual, txtDownload.Public)   // TODO: Should it be cleared?
				convey.So(metaDataDoc.Downloads.XLS.HRef, convey.ShouldEqual, xlsDownload.HRef)
				convey.So(metaDataDoc.Downloads.XLS.Size, convey.ShouldEqual, xlsDownload.Size)
				convey.So(metaDataDoc.Downloads.XLS.Private, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.XLS.Public, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Downloads.XLSX.Private, convey.ShouldEqual, xlsxDownload.Private) // TODO: Should it be cleared?
				convey.So(metaDataDoc.Downloads.XLSX.Public, convey.ShouldEqual, xlsxDownload.Public)   // TODO: Should it be cleared?

				convey.So(metaDataDoc.Links.Self.HRef, convey.ShouldEqual, version.Links.Version.HRef+"/metadata")
				convey.So(metaDataDoc.Links.Self.ID, convey.ShouldEqual, "")
				convey.So(metaDataDoc.Links.Spatial, convey.ShouldEqual, version.Links.Spatial)
				convey.So(metaDataDoc.Links.Version, convey.ShouldEqual, version.Links.Version)
				expectedWebsiteHref := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%d",
					websiteURL, dataset.ID, version.Links.Edition.ID, version.Version)
				convey.So(metaDataDoc.Links.WebsiteVersion.HRef, convey.ShouldEqual, expectedWebsiteHref)
				convey.So(metaDataDoc.Links.WebsiteVersion.ID, convey.ShouldEqual, "")
				convey.So(metaDataDoc.IsBasedOn, convey.ShouldResemble, &IsBasedOn{
					ID:   "UR_HH",
					Type: "All usual residents in households",
				})

				// TODO: Should it include xlsx?
				convey.So(metaDataDoc.Distribution, convey.ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})
			})

			convey.Convey("And the non-CMD fields are empty", func() {
				convey.So(metaDataDoc.CSVHeader, convey.ShouldBeNil)
				convey.So(metaDataDoc.DatasetLinks, convey.ShouldBeNil)
				convey.So(metaDataDoc.RelatedContent, convey.ShouldBeNil)
				convey.So(metaDataDoc.Version, convey.ShouldEqual, 0)
			})
		})
	})
}

func TestUpdateMetadata(t *testing.T) {
	convey.Convey("Given an EditableMetadata objects", t, func() {
		isNationalStatistic := true
		isAreaType := true
		numberOfOptions := 8
		metadata := EditableMetadata{
			Alerts: &[]Alert{{
				Date:        "alert-date",
				Description: "alert-description",
				Type:        "alert-type",
			}},
			CanonicalTopic: "topic",
			Contacts: []ContactDetails{{
				Email:     "email",
				Name:      "nane",
				Telephone: "telephone",
			}},
			Description: "description",
			Dimensions: []Dimension{{
				Description: "dim1-desc",
				Label:       "dim1-label",
				LastUpdated: time.Now(),
				Links: DimensionLink{
					CodeList: LinkObject{
						HRef: "codelist-url",
						ID:   "codelist-id",
					},
					Options: LinkObject{
						HRef: "options-url",
						ID:   "options-id",
					},
					Version: LinkObject{
						HRef: "version-url",
						ID:   "version-id",
					},
				},
				HRef:                 "dim1-url",
				ID:                   "dim1-id",
				Name:                 "dim1-name",
				Variable:             "dim1-var",
				NumberOfOptions:      &numberOfOptions,
				IsAreaType:           &isAreaType,
				QualityStatementText: "dim1-qs-text",
				QualityStatementURL:  "dim1-qs-url",
			}},
			Keywords: []string{"key", "word"},
			LatestChanges: &[]LatestChange{{
				Description: "latest-changes-desc",
				Name:        "latest-changes-name",
				Type:        "latest-changes-type",
			}},
			License: "license",
			Methodologies: []GeneralDetails{{
				Description: "methodlogies",
				HRef:        "methodologies-url",
				Title:       "methodologies-title",
			}},
			NationalStatistic: &isNationalStatistic,
			NextRelease:       "tomorrow",
			Publications: []GeneralDetails{{
				Description: "publications-desc",
				HRef:        "pub-url",
				Title:       "publications",
			}},
			QMI: &GeneralDetails{
				Description: "qmi-desc",
				HRef:        "qmi-url",
				Title:       "QMI",
			},
			RelatedDatasets: []GeneralDetails{{
				Description: "related-datasets-desc",
				HRef:        "related-datasets-url",
				Title:       "related-datasets",
			}},
			ReleaseDate:      "today",
			ReleaseFrequency: "daily",
			Title:            "title",
			Survey:           "census",
			Subtopics:        []string{"subtopic1", "subtopic2"},
			UnitOfMeasure:    "unit",
			UsageNotes: &[]UsageNote{{
				Note:  "usage note",
				Title: "usage note title",
			}},
			RelatedContent: []GeneralDetails{{
				Description: "related-content-desc",
				HRef:        "related-content-rul",
				Title:       "related-content-title",
			}},
		}

		collectionID := "collection-id"
		datasetID := "dataset-id"

		convey.Convey("And a dataset", func() {
			lastUpdated := time.Now()
			datasetType := "type"
			state := PublishedState
			theme := "population"
			uri := "dataset-uri"
			links := DatasetLinks{
				AccessRights: &LinkObject{
					HRef: "href-access-rights",
					ID:   "access-rights",
				},
				Editions: &LinkObject{
					HRef: "href-editions",
					ID:   "editions",
				},
				LatestVersion: &LinkObject{
					HRef: "href-latest",
					ID:   "latest",
				},
				Self: &LinkObject{
					HRef: "href-self",
					ID:   "self",
				},
				Taxonomy: &LinkObject{
					HRef: "href-taxonomy",
					ID:   "taxonomy",
				},
			}
			isBasedOn := IsBasedOn{
				ID:   "UR_HH",
				Type: "All usual residents in households",
			}
			dataset := Dataset{
				CollectionID: collectionID,
				ID:           datasetID,
				LastUpdated:  lastUpdated,
				Links:        &links,
				Publisher:    &publisher,
				State:        state,
				Theme:        theme,
				URI:          uri,
				Type:         datasetType,
				IsBasedOn:    &isBasedOn,
			}

			convey.Convey("When we call UpdateMetadata on the dataset", func() {
				dataset.UpdateMetadata(metadata)

				convey.Convey("Then all the metadata fields are updated correctly", func() {
					convey.So(dataset.CanonicalTopic, convey.ShouldEqual, metadata.CanonicalTopic)
					convey.So(dataset.Title, convey.ShouldEqual, metadata.Title)
					convey.So(dataset.Contacts, convey.ShouldResemble, metadata.Contacts)
					convey.So(dataset.NextRelease, convey.ShouldEqual, metadata.NextRelease)
					convey.So(dataset.License, convey.ShouldEqual, metadata.License)
					convey.So(dataset.Description, convey.ShouldEqual, metadata.Description)
					convey.So(dataset.UnitOfMeasure, convey.ShouldEqual, metadata.UnitOfMeasure)
					convey.So(dataset.Keywords, convey.ShouldResemble, metadata.Keywords)
					convey.So(dataset.Subtopics, convey.ShouldResemble, metadata.Subtopics)
					convey.So(dataset.RelatedContent, convey.ShouldResemble, metadata.RelatedContent)
					convey.So(dataset.NationalStatistic, convey.ShouldEqual, metadata.NationalStatistic)
					convey.So(dataset.Methodologies, convey.ShouldResemble, metadata.Methodologies)
					convey.So(dataset.QMI, convey.ShouldResemble, metadata.QMI)
					convey.So(dataset.ReleaseFrequency, convey.ShouldEqual, metadata.ReleaseFrequency)
					convey.So(dataset.RelatedDatasets, convey.ShouldResemble, metadata.RelatedDatasets)
					convey.So(dataset.Publications, convey.ShouldResemble, metadata.Publications)
					convey.So(dataset.Survey, convey.ShouldEqual, metadata.Survey)
				})
				convey.Convey("And none of the non-metadata fields is updated", func() {
					convey.So(dataset.CollectionID, convey.ShouldEqual, collectionID)
					convey.So(dataset.ID, convey.ShouldEqual, datasetID)
					convey.So(dataset.LastUpdated, convey.ShouldEqual, lastUpdated)
					convey.So(dataset.Links, convey.ShouldEqual, &links)
					convey.So(dataset.Publisher, convey.ShouldEqual, &publisher)
					convey.So(dataset.State, convey.ShouldEqual, state)
					convey.So(dataset.Theme, convey.ShouldEqual, theme)
					convey.So(dataset.URI, convey.ShouldEqual, uri)
					convey.So(dataset.Type, convey.ShouldEqual, datasetType)
					convey.So(dataset.IsBasedOn, convey.ShouldEqual, &isBasedOn)
				})
			})
		})

		convey.Convey("And a version", func() {
			downloads := DownloadList{
				CSV: &DownloadObject{
					HRef:    "https://www.aws/123csv",
					Private: "csv-private",
					Public:  "csv-public",
					Size:    "252",
				},
				TXT: &DownloadObject{
					HRef:    "https://www.aws/txt",
					Private: "txt-private",
					Public:  "txt-public",
					Size:    "11",
				},
			}
			edition := "2017"
			headers := []string{"cantabular_table", "age"}
			state := PublishedState
			temporalFrequencies := []TemporalFrequency{temporal}
			versionNumber := 1
			versionID := "65417"
			lastUpdated := time.Now()
			isBasedOn := IsBasedOn{
				ID:   "UR_HH",
				Type: "All usual residents in households",
			}
			versionType := "version-type"
			etag := "v-etag"
			lowestGeography := "low"
			version := Version{
				CollectionID:    collectionID,
				DatasetID:       datasetID,
				Downloads:       &downloads,
				Edition:         edition,
				Headers:         headers,
				ID:              versionID,
				LastUpdated:     lastUpdated,
				Links:           &links,
				State:           state,
				Temporal:        &temporalFrequencies,
				IsBasedOn:       &isBasedOn,
				Version:         versionNumber,
				Type:            versionType,
				ETag:            etag,
				LowestGeography: lowestGeography,
			}

			convey.Convey("When we call UpdateMetadata on the version", func() {
				version.UpdateMetadata(metadata)

				convey.Convey("Then all the metadata fields are updated correctly", func() {
					convey.So(version.ReleaseDate, convey.ShouldEqual, metadata.ReleaseDate)
					convey.So(version.Alerts, convey.ShouldResemble, metadata.Alerts)
					convey.So(version.Dimensions, convey.ShouldResemble, metadata.Dimensions)
					convey.So(version.UsageNotes, convey.ShouldResemble, metadata.UsageNotes)
					convey.So(version.LatestChanges, convey.ShouldResemble, metadata.LatestChanges)
				})

				convey.Convey("And none of the non-metadata fields is updated", func() {
					convey.So(version.CollectionID, convey.ShouldEqual, collectionID)
					convey.So(version.DatasetID, convey.ShouldEqual, datasetID)
					convey.So(version.Downloads, convey.ShouldEqual, &downloads)
					convey.So(version.Edition, convey.ShouldEqual, edition)
					convey.So(version.Headers, convey.ShouldResemble, headers)
					convey.So(version.ID, convey.ShouldEqual, versionID)
					convey.So(version.LastUpdated, convey.ShouldEqual, lastUpdated)
					convey.So(version.Links, convey.ShouldEqual, &links)
					convey.So(version.State, convey.ShouldEqual, state)
					convey.So(version.Temporal, convey.ShouldEqual, &temporalFrequencies)
					convey.So(version.IsBasedOn, convey.ShouldEqual, &isBasedOn)
					convey.So(version.Version, convey.ShouldEqual, versionNumber)
					convey.So(version.Type, convey.ShouldEqual, versionType)
					convey.So(version.ETag, convey.ShouldEqual, etag)
					convey.So(version.LowestGeography, convey.ShouldEqual, lowestGeography)
				})
			})
		})
	})
}
