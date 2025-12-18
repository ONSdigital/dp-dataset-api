package utils

import (
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateDistributionsFromRequestBody(t *testing.T) {
	Convey("Given a request body with distributions", t, func() {
		Convey("When distributions contain valid formats", func() {
			bodyBytes := []byte(`{"distributions": [{"format": "csv"}]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When distributions contain all supported format types", func() {
			bodyBytes := []byte(`{"distributions": [
				{"format": "csv"},
				{"format": "xls"},
				{"format": "xlsx"},
				{"format": "sdmx"},
				{"format": "csdb"},
				{"format": "csvw-metadata"}
			]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When a distribution is missing format field", func() {
			bodyBytes := []byte(`{"distributions": [{}]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[0].format field is missing")
			})
		})

		Convey("When a distribution has invalid format", func() {
			bodyBytes := []byte(`{"distributions": [{"format": "pdf"}]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[0].format field is invalid")
			})
		})

		Convey("When multiple distributions are provided", func() {
			bodyBytes := []byte(`{"distributions": [
				{"format": "csv"},
				{"format": "sdmx"}
			]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When format field is not a string", func() {
			bodyBytes := []byte(`{"distributions": [{"format": 123}]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[0].format field is invalid")
			})
		})

		Convey("When distributions field is not present", func() {
			bodyBytes := []byte(`{}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When distributions field is empty array", func() {
			bodyBytes := []byte(`{"distributions": []}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When request body is malformed JSON", func() {
			bodyBytes := []byte(`{"distributions": [{"format": "csv"`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				// The function returns nil on JSON parse errors to let main unmarshal handle it
				So(err, ShouldBeNil)
			})
		})

		Convey("When distributions field is not an array", func() {
			bodyBytes := []byte(`{"distributions": "not-an-array"}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then no error should be returned", func() {
				// The function returns nil when distributions is not an array to let main unmarshal handle it
				So(err, ShouldBeNil)
			})
		})

		Convey("When format field is empty string", func() {
			bodyBytes := []byte(`{"distributions": [{"format": ""}]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[0].format field is invalid")
			})
		})

		Convey("When second distribution has missing format", func() {
			bodyBytes := []byte(`{"distributions": [
				{"format": "csv"},
				{}
			]}`)
			err := ValidateDistributionsFromRequestBody(bodyBytes)

			Convey("Then an error should be returned for the second distribution", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[1].format field is missing")
			})
		})
	})
}

func TestPopulateDistributions(t *testing.T) {
	Convey("Given a version with distributions", t, func() {
		Convey("When distributions have CSV format", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{
					{Format: "csv", DownloadURL: "http://example.com/data.csv"},
				},
			}

			err := PopulateDistributions(version)

			Convey("Then the media type should be set correctly", func() {
				So(err, ShouldBeNil)
				So((*version.Distributions)[0].MediaType, ShouldEqual, models.DistributionMediaTypeCSV)
			})
		})

		Convey("When distributions have multiple formats", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{
					{Format: "csv", DownloadURL: "http://example.com/data.csv"},
					{Format: "sdmx", DownloadURL: "http://example.com/data.sdmx"},
					{Format: "xls", DownloadURL: "http://example.com/data.xls"},
					{Format: "xlsx", DownloadURL: "http://example.com/data.xlsx"},
					{Format: "csdb", DownloadURL: "http://example.com/data.csdb"},
					{Format: "csvw-metadata", DownloadURL: "http://example.com/data.json"},
				},
			}

			err := PopulateDistributions(version)

			Convey("Then all media types should be set correctly", func() {
				So(err, ShouldBeNil)
				So((*version.Distributions)[0].MediaType, ShouldEqual, models.DistributionMediaTypeCSV)
				So((*version.Distributions)[1].MediaType, ShouldEqual, models.DistributionMediaTypeSDMX)
				So((*version.Distributions)[2].MediaType, ShouldEqual, models.DistributionMediaTypeXLS)
				So((*version.Distributions)[3].MediaType, ShouldEqual, models.DistributionMediaTypeXLSX)
				So((*version.Distributions)[4].MediaType, ShouldEqual, models.DistributionMediaTypeCSDB)
				So((*version.Distributions)[5].MediaType, ShouldEqual, models.DistributionMediaTypeCSVWMeta)
			})
		})

		Convey("When version has nil distributions", func() {
			version := &models.Version{
				Distributions: nil,
			}

			err := PopulateDistributions(version)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When version has empty distributions array", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{},
			}

			err := PopulateDistributions(version)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When a distribution has missing format field", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{
					{DownloadURL: "http://example.com/data"},
				},
			}

			err := PopulateDistributions(version)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[0].format field is missing")
			})
		})

		Convey("When a distribution has invalid format", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{
					{Format: "pdf", DownloadURL: "http://example.com/data.pdf"},
				},
			}

			err := PopulateDistributions(version)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[0].format field is invalid")
			})
		})

		Convey("When second distribution has invalid format", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{
					{Format: "csv", DownloadURL: "http://example.com/data.csv"},
					{Format: "pdf", DownloadURL: "http://example.com/data.pdf"},
				},
			}

			err := PopulateDistributions(version)

			Convey("Then an error should be returned for the second distribution", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[1].format field is invalid")
			})
		})

		Convey("When third distribution has missing format", func() {
			version := &models.Version{
				Distributions: &[]models.Distribution{
					{Format: "csv", DownloadURL: "http://example.com/data.csv"},
					{Format: "sdmx", DownloadURL: "http://example.com/data.sdmx"},
					{DownloadURL: "http://example.com/data"},
				},
			}

			err := PopulateDistributions(version)

			Convey("Then an error should be returned for the third distribution", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "distributions[2].format field is missing")
			})
		})
	})
}
