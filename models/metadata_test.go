package models

import (
	"fmt"
	neturl "net/url"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

// Mock data used across tests
var (
	csvDownload = DownloadObject{
		HRef:    "https://www.aws/123csv",
		Private: "csv-private",
		Public:  "csv-public",
		Size:    "252",
	}
	csvwDownload = DownloadObject{
		HRef:    "https://www.aws/123",
		Private: "csvw-private",
		Public:  "csvw-public",
		Size:    "25",
	}
	xlsDownload = DownloadObject{
		HRef:    "https://www.aws/1234",
		Private: "xls-private",
		Public:  "xls-public",
		Size:    "45",
	}
	txtDownload = DownloadObject{
		HRef:    "https://www.aws/txt",
		Private: "txt-private",
		Public:  "txt-public",
		Size:    "11",
	}
	xlsxDownload = DownloadObject{
		HRef:    "https://www.aws/xlsx",
		Private: "xlsx-private",
		Public:  "xlsx-public",
		Size:    "119",
	}
)

func TestCreateMetadata(t *testing.T) {
	Convey("Given a dataset and a version objects", t, func() {
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

		Convey("When we call CreateCantabularMetaDataDoc", func() {
			metaDataDoc := CreateCantabularMetaDataDoc(&dataset, &version)

			Convey("Then it returns a metadata object with all the Cantabular fields populated", func() {
				So(metaDataDoc.Description, ShouldEqual, dataset.Description)
				So(metaDataDoc.Keywords, ShouldResemble, dataset.Keywords)
				So(metaDataDoc.Title, ShouldEqual, dataset.Title)
				So(metaDataDoc.UnitOfMeasure, ShouldEqual, dataset.UnitOfMeasure)
				So(metaDataDoc.Contacts, ShouldResemble, dataset.Contacts)
				So(metaDataDoc.URI, ShouldEqual, dataset.URI)
				So(metaDataDoc.QMI, ShouldResemble, dataset.QMI)
				So(metaDataDoc.DatasetLinks, ShouldResemble, dataset.Links)
				So(metaDataDoc.RelatedContent, ShouldResemble, dataset.RelatedContent)
				So(metaDataDoc.CanonicalTopic, ShouldEqual, dataset.CanonicalTopic)
				So(metaDataDoc.Subtopics, ShouldResemble, dataset.Subtopics)
				So(metaDataDoc.Topics, ShouldBeNil)

				So(metaDataDoc.CSVHeader, ShouldResemble, version.Headers)
				So(metaDataDoc.Dimensions, ShouldResemble, version.Dimensions)
				So(metaDataDoc.ReleaseDate, ShouldEqual, version.ReleaseDate)
				So(metaDataDoc.Version, ShouldEqual, version.Version)
				So(metaDataDoc.State, ShouldEqual, version.State)

				So(metaDataDoc.Downloads.CSV.HRef, ShouldEqual, csvDownload.HRef)
				So(metaDataDoc.Downloads.CSV.Size, ShouldEqual, csvDownload.Size)
				So(metaDataDoc.Downloads.CSV.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSV.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSVW.HRef, ShouldEqual, csvwDownload.HRef)
				So(metaDataDoc.Downloads.CSVW.Size, ShouldEqual, csvwDownload.Size)
				So(metaDataDoc.Downloads.CSVW.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSVW.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.HRef, ShouldEqual, txtDownload.HRef)
				So(metaDataDoc.Downloads.TXT.Size, ShouldEqual, txtDownload.Size)
				So(metaDataDoc.Downloads.TXT.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.HRef, ShouldEqual, xlsDownload.HRef)
				So(metaDataDoc.Downloads.XLS.Size, ShouldEqual, xlsDownload.Size)
				So(metaDataDoc.Downloads.XLS.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLSX.HRef, ShouldEqual, xlsxDownload.HRef)
				So(metaDataDoc.Downloads.XLSX.Size, ShouldEqual, xlsxDownload.Size)
				So(metaDataDoc.Downloads.XLSX.Private, ShouldEqual, xlsxDownload.Private) // TODO: Should it be cleared?
				So(metaDataDoc.Downloads.XLSX.Public, ShouldEqual, xlsxDownload.Public)   // TODO: Should it be cleared?
				So(metaDataDoc.IsBasedOn, ShouldResemble, &IsBasedOn{
					ID:   "UR_HH",
					Type: "All usual residents in households",
				})
				So(metaDataDoc.Version, ShouldEqual, 1)

				// TODO: Should it include xlsx?
				So(metaDataDoc.Distribution, ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})
			})

			Convey("And the non-Cantabular fields are empty", func() {
				So(metaDataDoc.Alerts, ShouldBeNil)
				So(metaDataDoc.LatestChanges, ShouldBeNil)
				So(metaDataDoc.Links, ShouldBeNil)
				So(metaDataDoc.License, ShouldEqual, "")
				So(metaDataDoc.Methodologies, ShouldBeNil)
				So(metaDataDoc.NationalStatistic, ShouldBeNil)
				So(metaDataDoc.NextRelease, ShouldEqual, "")
				So(metaDataDoc.Publications, ShouldBeNil)
				So(metaDataDoc.Publisher, ShouldBeNil)
				So(metaDataDoc.RelatedDatasets, ShouldBeNil)
				So(metaDataDoc.ReleaseFrequency, ShouldEqual, "")
				So(metaDataDoc.Temporal, ShouldBeNil)
				So(metaDataDoc.Theme, ShouldEqual, "")
				So(metaDataDoc.UsageNotes, ShouldBeNil)
			})
		})

		Convey("When we call CreateMetaDataDoc with a non-static dataset", func() {
			codeListAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:22400"}
			datasetAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:22000"}
			downloadServiceURL := &neturl.URL{Scheme: "http", Host: "localhost:23600"}
			importAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:21800"}
			websiteURL := &neturl.URL{Scheme: "http", Host: "localhost:20000"}
			urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)
			metaDataDoc := CreateMetaDataDoc(&dataset, &version, urlBuilder)

			Convey("Then it returns a metadata object with canonical topic and subtopics set", func() {
				So(metaDataDoc.CanonicalTopic, ShouldEqual, dataset.CanonicalTopic)
				So(metaDataDoc.Subtopics, ShouldResemble, dataset.Subtopics)
				So(metaDataDoc.Topics, ShouldBeNil)
			})

			Convey("And the state is set from the version", func() {
				So(metaDataDoc.State, ShouldEqual, version.State)
			})
		})

		Convey("When we call CreateMetaDataDoc with a static dataset", func() {
			// Create a copy of the dataset with type set to static
			staticDataset := dataset
			staticDataset.Type = "static"
			staticDataset.Topics = []string{"topic1", "topic2", "topic3"}

			codeListAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:22400"}
			datasetAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:22000"}
			downloadServiceURL := &neturl.URL{Scheme: "http", Host: "localhost:23600"}
			importAPIURL := &neturl.URL{Scheme: "http", Host: "localhost:21800"}
			websiteURL := &neturl.URL{Scheme: "http", Host: "localhost:20000"}
			urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)
			metaDataDoc := CreateMetaDataDoc(&staticDataset, &version, urlBuilder)

			Convey("Then it returns a metadata object with topics populated", func() {
				So(metaDataDoc.Topics, ShouldResemble, staticDataset.Topics)
			})

			Convey("And the state is set from the version", func() {
				So(metaDataDoc.State, ShouldEqual, version.State)
			})
		})
	})
}

func TestUpdateMetadata(t *testing.T) {
	Convey("Given an EditableMetadata objects", t, func() {
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
			Topics: []string{"topic1", "topic2"},
		}

		collectionID := "collection-id"
		datasetID := "dataset-id"

		Convey("And a non-static dataset", func() {
			lastUpdated := time.Now()
			datasetType := "cantabular_table"
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

			Convey("When we call UpdateMetadata on the dataset", func() {
				dataset.UpdateMetadata(metadata)

				Convey("Then all the metadata fields are updated correctly", func() {
					So(dataset.CanonicalTopic, ShouldEqual, metadata.CanonicalTopic)
					So(dataset.Title, ShouldEqual, metadata.Title)
					So(dataset.Contacts, ShouldResemble, metadata.Contacts)
					So(dataset.NextRelease, ShouldEqual, metadata.NextRelease)
					So(dataset.License, ShouldEqual, metadata.License)
					So(dataset.Description, ShouldEqual, metadata.Description)
					So(dataset.UnitOfMeasure, ShouldEqual, metadata.UnitOfMeasure)
					So(dataset.Keywords, ShouldResemble, metadata.Keywords)
					So(dataset.Subtopics, ShouldResemble, metadata.Subtopics)
					So(dataset.RelatedContent, ShouldResemble, metadata.RelatedContent)
					So(dataset.NationalStatistic, ShouldEqual, metadata.NationalStatistic)
					So(dataset.Methodologies, ShouldResemble, metadata.Methodologies)
					So(dataset.QMI, ShouldResemble, metadata.QMI)
					So(dataset.ReleaseFrequency, ShouldEqual, metadata.ReleaseFrequency)
					So(dataset.RelatedDatasets, ShouldResemble, metadata.RelatedDatasets)
					So(dataset.Publications, ShouldResemble, metadata.Publications)
					So(dataset.Survey, ShouldEqual, metadata.Survey)
					So(dataset.Topics, ShouldBeNil)
				})
				Convey("And none of the non-metadata fields is updated", func() {
					So(dataset.CollectionID, ShouldEqual, collectionID)
					So(dataset.ID, ShouldEqual, datasetID)
					So(dataset.LastUpdated, ShouldEqual, lastUpdated)
					So(dataset.Links, ShouldEqual, &links)
					So(dataset.Publisher, ShouldEqual, &publisher)
					So(dataset.State, ShouldEqual, state)
					So(dataset.Theme, ShouldEqual, theme)
					So(dataset.URI, ShouldEqual, uri)
					So(dataset.Type, ShouldEqual, datasetType)
					So(dataset.IsBasedOn, ShouldEqual, &isBasedOn)
				})
			})
		})

		Convey("And a static dataset", func() {
			lastUpdated := time.Now()
			datasetType := "static"
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

			Convey("When we call UpdateMetadata on the dataset", func() {
				dataset.UpdateMetadata(metadata)

				Convey("Then all the metadata fields including topics are updated correctly", func() {
					So(dataset.CanonicalTopic, ShouldEqual, metadata.CanonicalTopic)
					So(dataset.Title, ShouldEqual, metadata.Title)
					So(dataset.Contacts, ShouldResemble, metadata.Contacts)
					So(dataset.NextRelease, ShouldEqual, metadata.NextRelease)
					So(dataset.License, ShouldEqual, metadata.License)
					So(dataset.Description, ShouldEqual, metadata.Description)
					So(dataset.UnitOfMeasure, ShouldEqual, metadata.UnitOfMeasure)
					So(dataset.Keywords, ShouldResemble, metadata.Keywords)
					So(dataset.Subtopics, ShouldResemble, metadata.Subtopics)
					So(dataset.RelatedContent, ShouldResemble, metadata.RelatedContent)
					So(dataset.NationalStatistic, ShouldEqual, metadata.NationalStatistic)
					So(dataset.Methodologies, ShouldResemble, metadata.Methodologies)
					So(dataset.QMI, ShouldResemble, metadata.QMI)
					So(dataset.ReleaseFrequency, ShouldEqual, metadata.ReleaseFrequency)
					So(dataset.RelatedDatasets, ShouldResemble, metadata.RelatedDatasets)
					So(dataset.Publications, ShouldResemble, metadata.Publications)
					So(dataset.Survey, ShouldEqual, metadata.Survey)
					So(dataset.Topics, ShouldResemble, metadata.Topics)
				})
			})
		})

		Convey("And a version", func() {
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

			Convey("When we call UpdateMetadata on the version", func() {
				version.UpdateMetadata(metadata)

				Convey("Then all the metadata fields are updated correctly", func() {
					So(version.ReleaseDate, ShouldEqual, metadata.ReleaseDate)
					So(version.Alerts, ShouldResemble, metadata.Alerts)
					So(version.Dimensions, ShouldResemble, metadata.Dimensions)
					So(version.UsageNotes, ShouldResemble, metadata.UsageNotes)
					So(version.LatestChanges, ShouldResemble, metadata.LatestChanges)
				})

				Convey("And none of the non-metadata fields is updated", func() {
					So(version.CollectionID, ShouldEqual, collectionID)
					So(version.DatasetID, ShouldEqual, datasetID)
					So(version.Downloads, ShouldEqual, &downloads)
					So(version.Edition, ShouldEqual, edition)
					So(version.Headers, ShouldResemble, headers)
					So(version.ID, ShouldEqual, versionID)
					So(version.LastUpdated, ShouldEqual, lastUpdated)
					So(version.Links, ShouldEqual, &links)
					So(version.State, ShouldEqual, state)
					So(version.Temporal, ShouldEqual, &temporalFrequencies)
					So(version.IsBasedOn, ShouldEqual, &isBasedOn)
					So(version.Version, ShouldEqual, versionNumber)
					So(version.Type, ShouldEqual, versionType)
					So(version.ETag, ShouldEqual, etag)
					So(version.LowestGeography, ShouldEqual, lowestGeography)
				})
			})
		})
	})
}

func TestMetadataToString(t *testing.T) {
	Convey("If metadata model is empty", t, func() {
		Convey("Test that the `ToString()` method returns the correct string", func() {
			m := Metadata{}
			expectedString := "Title: \n" +
				"Description: \n" +
				"Issued: \n" +
				"Next Release: \n" +
				"Identifier: \n" +
				"Language: English\n" +
				"Periodicity: \n" +
				"Distribution:\n" +
				"Unit of measure: \n" +
				"License: \n" +
				"National Statistic: false\n" +
				"Canonical Topic: \n" +
				"Survey: \n"

			So(m.ToString(), ShouldEqual, expectedString)
		})
	})
	Convey("If metadata model is not empty", t, func() {
		nationalStatistic := false
		m := Metadata{
			EditableMetadata: EditableMetadata{
				CanonicalTopic:    "canon",
				Description:       "metadata description",
				License:           "MIT",
				NationalStatistic: &nationalStatistic,
				NextRelease:       "2025-06-06T16:06:00.000Z",
				ReleaseDate:       "2025-05-06T16:06:00.000Z",
				ReleaseFrequency:  "monthly",
				Survey:            "my survey",
				Title:             "metadata title",
				UnitOfMeasure:     "miles",
			},
			Version: 1,
		}
		Convey("Test that the `ToString()` method returns the correct string", func() {
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains publisher details", func() {
			// Update the model to include publisher
			m.Publisher = &Publisher{
				HRef: "url",
				Name: "name",
				Type: "type",
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Publisher: %s\n", *m.Publisher) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})

		Convey("Test that the `ToString()` method contains keywords", func() {
			// Update the model to include keywords
			m.Keywords = []string{"key1", "key2", "key3"}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				fmt.Sprintf("Keywords: %s\n", m.Keywords) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})

		Convey("Test that the `ToString()` method contains contact details", func() {
			// Update the model to include contacts
			m.Contacts = []ContactDetails{
				{
					Email:     "test.user@ons.gov.uk",
					Name:      "Test User",
					Telephone: "01234 567890",
				},
				{
					Email:     "my.user@ons.gov.uk",
					Name:      "My User",
					Telephone: "01098 765432",
				},
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Contact: %s, %s, %s\n", m.Contacts[0].Name, m.Contacts[0].Email, m.Contacts[0].Telephone) +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains temporal information", func() {
			// Update the model to include temporal string
			temporal1 := TemporalFrequency{
				EndDate:   "2025-06-06T16:06:00.000Z",
				Frequency: "every day",
				StartDate: "2025-05-06T16:06:00.000Z",
			}
			temporal2 := TemporalFrequency{
				EndDate:   "2025-06-06T16:06:00.000Z",
				Frequency: "every week",
				StartDate: "2025-05-06T16:06:00.000Z",
			}
			m.Temporal = &[]TemporalFrequency{
				temporal1,
				temporal2,
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Temporal: %s\n", temporal1.Frequency) +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains latest changes", func() {
			// Update the model to include latest changes
			m.LatestChanges = &[]LatestChange{
				{
					Description: "my 1st change",
					Name:        "change-1",
					Type:        "revision",
				},
				{
					Description: "my 2nd change",
					Name:        "change-2",
					Type:        "correction",
				},
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Latest Changes: %s\n", *m.LatestChanges) +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains downloads information", func() {
			// Update the model to include downloads
			m.Downloads = &DownloadList{
				CSV:  &csvDownload,
				CSVW: &csvwDownload,
				TXT:  &txtDownload,
				XLS:  &xlsDownload,
				XLSX: &xlsxDownload,
			}

			// Using `ShouldContainSubstring` here as we can't guarantee the ordering of download files output
			expectedStringCsv := fmt.Sprintf("\tExtension: %s\n", "csv") +
				fmt.Sprintf("\tSize: %s\n", m.Downloads.CSV.Size) +
				fmt.Sprintf("\tURL: %s\n\n", m.Downloads.CSV.HRef)
			So(m.ToString(), ShouldContainSubstring, expectedStringCsv)

			expectedStringCsvw := fmt.Sprintf("\tExtension: %s\n", "csvw") +
				fmt.Sprintf("\tSize: %s\n", m.Downloads.CSVW.Size) +
				fmt.Sprintf("\tURL: %s\n\n", m.Downloads.CSVW.HRef)
			So(m.ToString(), ShouldContainSubstring, expectedStringCsvw)

			expectedStringTxt := fmt.Sprintf("\tExtension: %s\n", "txt") +
				fmt.Sprintf("\tSize: %s\n", m.Downloads.TXT.Size) +
				fmt.Sprintf("\tURL: %s\n\n", m.Downloads.TXT.HRef)
			So(m.ToString(), ShouldContainSubstring, expectedStringTxt)

			expectedStringXls := fmt.Sprintf("\tExtension: %s\n", "xls") +
				fmt.Sprintf("\tSize: %s\n", m.Downloads.XLS.Size) +
				fmt.Sprintf("\tURL: %s\n\n", m.Downloads.XLS.HRef)
			So(m.ToString(), ShouldContainSubstring, expectedStringXls)

			expectedStringXlsx := fmt.Sprintf("\tExtension: %s\n", "xlsx") +
				fmt.Sprintf("\tSize: %s\n", m.Downloads.XLSX.Size) +
				fmt.Sprintf("\tURL: %s\n\n", m.Downloads.XLSX.HRef)
			So(m.ToString(), ShouldContainSubstring, expectedStringXlsx)
		})
		Convey("Test that the `ToString()` method contains methodology information", func() {
			// Update the model to include methodology
			m.Methodologies = []GeneralDetails{
				{
					Description: "some methodologies description",
					HRef:        "http://localhost:22000/datasets/href",
					Title:       "some publication title",
				},
				{
					Description: "another methodologies description",
					HRef:        "http://localhost:22000/datasets/hruf",
					Title:       "another publication title",
				},
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("Methodologies: %s\n", m.Methodologies) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains publication information", func() {
			// Update the model to include publication
			m.Publications = []GeneralDetails{
				{
					Description: "some publication description",
					HRef:        "http://localhost:22000/datasets/href",
					Title:       "some publication title",
				},
				{
					Description: "another publication description",
					HRef:        "http://localhost:22000/datasets/hruf",
					Title:       "another publication title",
				},
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Publications: %s\n", m.Publications) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains related dataset information", func() {
			// Update the model to include related datasets
			m.RelatedDatasets = []GeneralDetails{
				{
					Description: "some related datasets description",
					HRef:        "http://localhost:22000/datasets/href",
					Title:       "some publication title",
				},
			}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Related Links: %s\n", m.RelatedDatasets) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains subtopic information", func() {
			// Update the model to include subtopics
			m.Subtopics = []string{"1234", "5678"}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Subtopics: %s\n", m.Subtopics) +
				fmt.Sprintf("Survey: %s\n", m.Survey)

			So(m.ToString(), ShouldEqual, expectedString)
		})
		Convey("Test that the `ToString()` method contains related content information", func() {
			// Update the model to include related content
			m.RelatedContent = []GeneralDetails{relatedDatasets}
			expectedString := fmt.Sprintf("Title: %s\n", m.Title) +
				fmt.Sprintf("Description: %s\n", m.Description) +
				fmt.Sprintf("Issued: %s\n", m.ReleaseDate) +
				fmt.Sprintf("Next Release: %s\n", m.NextRelease) +
				fmt.Sprintf("Identifier: %s\n", m.Title) +
				"Language: English\n" +
				fmt.Sprintf("Periodicity: %s\n", m.ReleaseFrequency) +
				"Distribution:\n" +
				fmt.Sprintf("Unit of measure: %s\n", m.UnitOfMeasure) +
				fmt.Sprintf("License: %s\n", m.License) +
				fmt.Sprintf("National Statistic: %v\n", nationalStatistic) +
				fmt.Sprintf("Canonical Topic: %s\n", m.CanonicalTopic) +
				fmt.Sprintf("Survey: %s\n", m.Survey) +
				fmt.Sprintf("Related Content: %s\n", m.RelatedContent)

			So(m.ToString(), ShouldEqual, expectedString)
		})
	})
}
