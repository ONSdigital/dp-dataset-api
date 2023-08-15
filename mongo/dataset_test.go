package mongo

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	id          = "123"
	editionID   = "2017"
	state       = models.PublishedState
	versionID   = 2
	testContext = context.Background()
)

func TestBuildEditionsQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set and the request is authorised then the selector only queries by id", t, func() {
		selector := buildEditionsQuery(id, "", true)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldHaveLength, 1)
		So(selector["next.links.dataset.id"], ShouldEqual, id)
	})

	Convey("When no state was set and the request is not authorised then the selector queries by id and current must exist", t, func() {
		selector := buildEditionsQuery(id, "", false)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldHaveLength, 2)
		So(selector["next.links.dataset.id"], ShouldEqual, id)
		So(selector["current"], ShouldResemble, bson.M{"$exists": true})
	})

	Convey("When state was set to published and request is authorised then the selector queries by id and state", t, func() {
		selector := buildEditionsQuery(id, state, true)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldHaveLength, 2)
		So(selector["next.links.dataset.id"], ShouldEqual, id)
		So(selector["current.state"], ShouldEqual, state)
	})

	Convey("When state was set to published and request is not authorised then the selector queries by id, state and current must exist", t, func() {
		selector := buildEditionsQuery(id, state, false)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldHaveLength, 3)
		So(selector["next.links.dataset.id"], ShouldEqual, id)
		So(selector["current.state"], ShouldEqual, state)
		So(selector["current"], ShouldResemble, bson.M{"$exists": true})
	})
}

func TestBuildEditionQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {
		expectedSelector := bson.M{
			"next.links.dataset.id": id,
			"next.edition":          editionID,
		}

		selector := buildEditionQuery(id, editionID, "")
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {
		expectedSelector := bson.M{
			"current.links.dataset.id": id,
			"current.edition":          editionID,
			"current.state":            state,
		}

		selector := buildEditionQuery(id, editionID, state)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestBuildVersionsQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {
		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"$or": []interface{}{
				bson.M{"state": "edition-confirmed"},
				bson.M{"state": "associated"},
				bson.M{"state": "published"},
			},
		}

		selector := buildVersionsQuery(id, editionID, "")
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {
		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}

		selector := buildVersionsQuery(id, editionID, state)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestBuildVersionQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {
		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
		}

		selector := buildVersionQuery(id, editionID, "", versionID)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {
		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
			"state":            state,
		}

		selector := buildVersionQuery(id, editionID, state, versionID)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestDatasetUpdateQuery(t *testing.T) {
	t.Parallel()
	Convey("When all possible fields exist", t, func() {
		contact := models.ContactDetails{
			Email:     "njarrod@test.com",
			Name:      "natalie jarrod",
			Telephone: "01658 234567",
		}

		var contacts []models.ContactDetails
		contacts = append(contacts, contact)

		methodology := models.GeneralDetails{
			Description: "some methodology description",
			HRef:        "http://localhost:22000//datasets/123/methodologies",
			Title:       "some methodology title",
		}

		publication := models.GeneralDetails{
			Description: "some publication description",
			HRef:        "http://localhost:22000//datasets/123/publications",
			Title:       "some publication title",
		}

		qmi := models.GeneralDetails{
			Description: "some qmi description",
			HRef:        "http://localhost:22000//datasets/123/qmi",
			Title:       "some qmi title",
		}

		relatedDataset := models.GeneralDetails{
			HRef:  "http://localhost:22000//datasets/432",
			Title: "some dataset title",
		}

		canonicalTopic := "canonicalTopicID"

		subtopics := []string{"secondaryTopic1ID", "secondaryTopic2ID"}

		survey := "mockSurvey"

		relatedContent := []models.GeneralDetails{{
			Description: "related content description 1",
			HRef:        "http://localhost:22000//datasets/123/relatedContent1",
			Title:       "Related content 1",
		}, {
			Description: "related content description 2",
			HRef:        "http://localhost:22000//datasets/123/relatedContent2",
			Title:       "Related content 2",
		}}

		var methodologies, publications, relatedDatasets []models.GeneralDetails
		methodologies = append(methodologies, methodology)
		publications = append(publications, publication)
		relatedDatasets = append(relatedDatasets, relatedDataset)
		nationalStatistic := true

		expectedUpdate := bson.M{
			"next.collection_id":            "12345678",
			"next.contacts":                 contacts,
			"next.description":              "test description",
			"next.keywords":                 []string{"statistics", "national"},
			"next.license":                  "ONS License",
			"next.links.access_rights.href": "http://ons.gov.uk/accessrights",
			"next.methodologies":            methodologies,
			"next.national_statistic":       &nationalStatistic,
			"next.next_release":             "2018-05-05",
			"next.publications":             publications,
			"next.publisher.href":           "http://ons.gov.uk",
			"next.publisher.name":           "Office of National Statistics",
			"next.publisher.type":           "Public",
			"next.qmi.description":          "some qmi description",
			"next.qmi.href":                 "http://localhost:22000//datasets/123/qmi",
			"next.qmi.title":                "some qmi title",
			"next.related_datasets":         relatedDatasets,
			"next.release_frequency":        "yearly",
			"next.theme":                    "construction",
			"next.title":                    "CPI",
			"next.uri":                      "http://ons.gov.uk/datasets/123/landing-page",
			"next.type":                     "nomis",
			"next.nomis_reference_url":      "https://www.nomisweb.co.uk/census/2011/ks106ew",
			"next.canonical_topic":          canonicalTopic,
			"next.subtopics":                subtopics,
			"next.survey":                   survey,
			"next.related_content":          relatedContent,
		}

		dataset := &models.Dataset{
			Contacts:     contacts,
			CollectionID: "12345678",
			Description:  "test description",
			Keywords:     []string{"statistics", "national"},
			License:      "ONS License",
			Links: &models.DatasetLinks{
				AccessRights: &models.LinkObject{
					HRef: "http://ons.gov.uk/accessrights",
				},
			},
			Methodologies:     methodologies,
			NationalStatistic: &nationalStatistic,
			NextRelease:       "2018-05-05",
			Publications:      publications,
			Publisher: &models.Publisher{
				Name: "Office of National Statistics",
				Type: "Public",
				HRef: "http://ons.gov.uk",
			},
			QMI:               &qmi,
			RelatedDatasets:   relatedDatasets,
			ReleaseFrequency:  "yearly",
			Theme:             "construction",
			Title:             "CPI",
			URI:               "http://ons.gov.uk/datasets/123/landing-page",
			Type:              "nomis",
			NomisReferenceURL: "https://www.nomisweb.co.uk/census/2011/ks106ew",
			CanonicalTopic:    canonicalTopic,
			Subtopics:         subtopics,
			Survey:            survey,
			RelatedContent:    relatedContent,
		}

		selector := createDatasetUpdateQuery(testContext, "123", dataset, models.CreatedState)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedUpdate)
	})

	Convey("When national statistic is set to false", t, func() {
		nationalStatistic := false
		dataset := &models.Dataset{
			NationalStatistic: &nationalStatistic,
		}

		expectedUpdate := bson.M{
			"next.national_statistic": &nationalStatistic,
		}

		selector := createDatasetUpdateQuery(testContext, "123", dataset, models.CreatedState)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedUpdate)
	})
}

func TestVersionUpdateQuery(t *testing.T) {
	t.Parallel()
	Convey("When all possible fields exist", t, func() {
		temporal := models.TemporalFrequency{
			EndDate:   "2017-09-09",
			Frequency: "monthly",
			StartDate: "2014-09-09",
		}

		expectedUpdate := bson.M{
			"collection_id":      "12345678",
			"release_date":       "2017-09-09",
			"links.spatial.href": "http://ons.gov.uk/geographylist",
			"state":              models.PublishedState,
			"temporal":           &[]models.TemporalFrequency{temporal},
			"e_tag":              "newETag",
		}

		version := &models.Version{
			CollectionID: "12345678",
			ReleaseDate:  "2017-09-09",
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
			State:    models.PublishedState,
			Temporal: &[]models.TemporalFrequency{temporal},
		}

		selector := createVersionUpdateQuery(version, "newETag")
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedUpdate)
	})
}
