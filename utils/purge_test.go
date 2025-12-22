package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGeneratePurgePrefixes(t *testing.T) {
	Convey("Given a website URL, API router public URL, dataset ID, edition and version", t, func() {
		websiteURL := "https://www.example.com"
		apiRouterPublicURL := "https://api.example.com"
		datasetID := "dataset123"
		edition := "2025"
		version := "1"

		Convey("When GeneratePurgePrefixes is called", func() {
			prefixes := GeneratePurgePrefixes(websiteURL, apiRouterPublicURL, datasetID, edition, version)

			Convey("Then the correct list of URL prefixes is returned", func() {
				expectedPrefixes := []string{
					"https://www.example.com/datasets/dataset123",
					"https://www.example.com/datasets/dataset123/editions",
					"https://www.example.com/datasets/dataset123/editions/2025/versions",
					"https://api.example.com/datasets/dataset123",
					"https://api.example.com/datasets/dataset123/editions",
					"https://api.example.com/datasets/dataset123/editions/2025/versions",
				}
				So(prefixes, ShouldResemble, expectedPrefixes)
			})
		})
	})
}
