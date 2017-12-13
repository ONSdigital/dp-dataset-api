package models

import "strings"

const (
	collectionID = "12345678"
)

var contacts = ContactDetails{
	Email:     "test@test.co.uk",
	Name:      "john test",
	Telephone: "01654 765432",
}

var methodology = GeneralDetails{
	Description: "some methodology description",
	HRef:        "http://localhost:22000//datasets/methodologies",
	Title:       "some methodology title",
}

var nationalStatistic = true

var publications = GeneralDetails{
	Description: "some publication description",
	HRef:        "http://localhost:22000//datasets/publications",
	Title:       "some publication title",
}

var publisher = Publisher{
	Name: "The office of national statistics",
	Type: "government",
	HRef: "https://www.ons.gov.uk/",
}

var qmi = GeneralDetails{
	Description: "some qmi description",
	HRef:        "http://localhost:22000//datasets/123/qmi",
	Title:       "Quality and Methodology Information",
}

var relatedDatasets = GeneralDetails{
	HRef:  "http://localhost:22000//datasets/124",
	Title: "Census Age",
}

// Create a fully populated dataset object to use in testing.
func createTestDataset() *Dataset {
	return &Dataset{
		ID:           "123",
		CollectionID: collectionID,
		Contacts: []ContactDetails{
			contacts,
		},
		Description: "census",
		Keywords:    []string{"test", "test2"},
		License:     "Office of National Statistics license",
		Links: &DatasetLinks{
			AccessRights: &LinkObject{
				HRef: "http://ons.gov.uk/accessrights",
			},
		},
		Methodologies: []GeneralDetails{
			methodology,
		},
		NationalStatistic: &nationalStatistic,
		NextRelease:       "2016-05-05",
		Publications: []GeneralDetails{
			publications,
		},
		Publisher: &publisher,
		QMI:       &qmi,
		RelatedDatasets: []GeneralDetails{
			relatedDatasets,
		},
		ReleaseFrequency: "yearly",
		State:            AssociatedState,
		Theme:            "population",
		Title:            "CensusEthnicity",
		UnitOfMeasure:    "Pounds Sterling",
		URI:              "http://localhost:22000/datasets/123/breadcrumbs",
	}
}

var alert = Alert{
	Date:        "2017-10-10",
	Description: "A correction to an observation for males of age 25, previously 11 now changed to 12",
	Type:        "Correction",
}

func expectedDataset() Dataset {
	return Dataset{
		CollectionID: collectionID,
		Contacts:     []ContactDetails{contacts},
		Description:  "census",
		Keywords:     []string{"test", "test2"},
		License:      "Office of National Statistics license",
		Links: &DatasetLinks{
			AccessRights: &LinkObject{
				HRef: "http://ons.gov.uk/accessrights",
			},
		},
		Methodologies:     []GeneralDetails{methodology},
		NationalStatistic: &nationalStatistic,
		NextRelease:       "2016-05-05",
		Publications:      []GeneralDetails{publications},
		Publisher:         &publisher,
		QMI:               &qmi,
		RelatedDatasets:   []GeneralDetails{relatedDatasets},
		ReleaseFrequency:  "yearly",
		State:             AssociatedState,
		Theme:             "population",
		Title:             "CensusEthnicity",
		UnitOfMeasure:     "Pounds Sterling",
		URI:               "http://localhost:22000/datasets/123/breadcrumbs",
	}
}

var dimension = CodeList{
	Description: "A list of ages between 18 and 75+",
	HRef:        "http://localhost:22400/codelists/1245",
	ID:          "1245",
	Name:        "age",
}

var downloads = DownloadList{
	CSV: &DownloadObject{
		URL:  "https://www.aws/123",
		Size: "25",
	},
	XLS: &DownloadObject{
		URL:  "https://www.aws/1234",
		Size: "45",
	},
}

var latestChange = LatestChange{
	Description: "The border of Southampton changed after the south east cliff face fell into the sea.",
	Name:        "Changes in Classification",
	Type:        "Summary of Changes",
}

var links = VersionLinks{
	Dataset: &LinkObject{
		HRef: "http://localhost:22000/datasets/123",
		ID:   "3265vj48317tr4r34r3f",
	},
	Dimensions: &LinkObject{
		HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
	},
	Edition: &LinkObject{
		HRef: "http://localhost:22000/datasets/123/editions/2017",
		ID:   "2017",
	},
	Self: &LinkObject{
		HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
	},
	Spatial: &LinkObject{
		HRef: "http://ons.gov.uk/geographylist",
	},
	Version: &LinkObject{
		ID:   "1",
		HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
	},
}

var temporal = TemporalFrequency{
	EndDate:   "2017-09-09",
	Frequency: "monthly",
	StartDate: "2014-09-09",
}

var editionConfirmedVersion = Version{
	Dimensions:  []CodeList{dimension},
	Downloads:   &downloads,
	Edition:     "2017",
	Links:       &links,
	ReleaseDate: "2016-04-04",
	State:       EditionConfirmedState,
	Version:     1,
}

var associatedVersion = Version{
	CollectionID:  collectionID,
	Dimensions:    []CodeList{dimension},
	Downloads:     &downloads,
	Edition:       "2017",
	LatestChanges: &[]LatestChange{latestChange},
	Links:         &links,
	ReleaseDate:   "2017-10-12",
	State:         AssociatedState,
	Temporal:      &[]TemporalFrequency{temporal},
	Version:       1,
}

var publishedVersion = Version{
	Alerts:        &[]Alert{alert},
	CollectionID:  collectionID,
	Dimensions:    []CodeList{dimension},
	Downloads:     &downloads,
	Edition:       "2017",
	LatestChanges: &[]LatestChange{latestChange},
	Links:         &links,
	ReleaseDate:   "2017-10-12",
	State:         PublishedState,
	Temporal:      &[]TemporalFrequency{temporal},
	Version:       1,
}

var badInputData = struct {
	CollectionID int `json:"collection_id"`
}{
	CollectionID: 1,
}

func expectedMetadataDoc() Metadata {
	return Metadata{
		Alerts:        &[]Alert{alert},
		Contacts:      []ContactDetails{contacts},
		Description:   "census",
		Dimensions:    []CodeList{dimension},
		Distribution:  []string{"json", "csv", "xls"},
		Downloads:     &downloads,
		Keywords:      []string{"test", "test2"},
		LatestChanges: &[]LatestChange{latestChange},
		License:       "Office of National Statistics license",
		Links: &MetadataLinks{
			AccessRights: &LinkObject{
				HRef: "http://ons.gov.uk/accessrights",
			},
			Self: &LinkObject{
				HRef: links.Version.HRef + "/metadata",
			},
			Spatial:        links.Spatial,
			Version:        links.Version,
			WebsiteVersion: &LinkObject{HRef: strings.Replace(links.Version.HRef, "22000", "20000", -1)},
		},
		Methodologies:     []GeneralDetails{methodology},
		NationalStatistic: &nationalStatistic,
		NextRelease:       "2016-05-05",
		Publications:      []GeneralDetails{publications},
		Publisher:         &publisher,
		QMI:               &qmi,
		RelatedDatasets:   []GeneralDetails{relatedDatasets},
		ReleaseDate:       "2017-10-12",
		ReleaseFrequency:  "yearly",
		Temporal:          &[]TemporalFrequency{temporal},
		Theme:             "population",
		Title:             "CensusEthnicity",
		UnitOfMeasure:     "Pounds Sterling",
		URI:               "http://localhost:22000/datasets/123/breadcrumbs",
	}
}
