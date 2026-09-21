package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGeneratePurgePrefixes(t *testing.T) {
	Convey("Given a public website URL, API router public URL, topic slug, dataset ID, edition and version", t, func() {
		publicWebsiteURL := "https://www.example.com"
		apiRouterPublicURL := "https://api.example.com"
		topicSlug := "economy"
		datasetID := "dataset123"
		edition := "2025"
		version := "1"

		Convey("When GeneratePurgePrefixes is called", func() {
			prefixes := GeneratePurgePrefixes(publicWebsiteURL, apiRouterPublicURL, topicSlug, datasetID, edition, version)

			Convey("Then the correct list of URL prefixes is returned", func() {
				expectedPrefixes := []string{
					"www.example.com/economy/datasets/dataset123",
					"api.example.com/datasets/dataset123",
				}
				So(prefixes, ShouldResemble, expectedPrefixes)
			})
		})
	})
}

func TestRemoveSchemeFromURL(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		expected string
	}{
		{
			name:     "removes https scheme",
			rawURL:   "https://www.example.com",
			expected: "www.example.com",
		},
		{
			name:     "removes http scheme",
			rawURL:   "http://www.example.com",
			expected: "www.example.com",
		},
		{
			name:     "preserves URL without scheme",
			rawURL:   "www.example.com",
			expected: "www.example.com",
		},
	}

	for _, test := range tests {
		Convey("Given a URL that "+test.name, t, func() {
			Convey("When removeSchemeFromURL is called", func() {
				result := removeSchemeFromURL(test.rawURL)

				Convey("Then the expected URL is returned", func() {
					So(result, ShouldEqual, test.expected)
				})
			})
		})
	}
}
