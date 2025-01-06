package url_test

import (
	"fmt"
	neturl "net/url"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/smartystreets/goconvey/convey"
)

var (
	websiteURL, _         = neturl.Parse("http://localhost:20000")
	downloadServiceURL, _ = neturl.Parse("http://localhost:23600")
	datasetAPIURL, _      = neturl.Parse("http://localhost:22000")
	codeListAPIURL, _     = neturl.Parse("http://localhost:22400")
	importAPIURL, _       = neturl.Parse("http://localhost:21800")
)

const (
	datasetID = "123"
	edition   = "2017"
	version   = "1"
)

func TestBuilder_BuildWebsiteDatasetVersionURL(t *testing.T) {
	convey.Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)

		convey.Convey("When BuildWebsiteDatasetVersionURL is called", func() {
			builtURL := urlBuilder.BuildWebsiteDatasetVersionURL(datasetID, edition, version)

			expectedURL := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
				websiteURL.String(), datasetID, edition, version)

			convey.Convey("Then the expected URL is returned", func() {
				convey.So(builtURL, convey.ShouldEqual, expectedURL)
			})
		})
	})
}

func TestBuilder_GetWebsiteURL(t *testing.T) {
	convey.Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)

		convey.Convey("When GetWebsiteURL is called", func() {
			returnedURL := urlBuilder.GetWebsiteURL()

			convey.Convey("Then the expected URL is returned", func() {
				convey.So(returnedURL, convey.ShouldEqual, websiteURL)
			})
		})
	})
}

func TestBuilder_GetDownloadServiceURL(t *testing.T) {
	convey.Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)

		convey.Convey("When GetDownloadServiceURL is called", func() {
			returnedURL := urlBuilder.GetDownloadServiceURL()

			convey.Convey("Then the expected URL is returned", func() {
				convey.So(returnedURL, convey.ShouldEqual, downloadServiceURL)
			})
		})
	})
}

func TestBuilder_GetDatasetAPIURL(t *testing.T) {
	convey.Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)

		convey.Convey("When GetDatasetAPIURL is called", func() {
			returnedURL := urlBuilder.GetDatasetAPIURL()

			convey.Convey("Then the expected URL is returned", func() {
				convey.So(returnedURL, convey.ShouldEqual, datasetAPIURL)
			})
		})
	})
}

func TestBuilder_GetCodeListAPIURL(t *testing.T) {
	convey.Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)

		convey.Convey("When GetCodeListAPIURL is called", func() {
			returnedURL := urlBuilder.GetCodeListAPIURL()

			convey.Convey("Then the expected URL is returned", func() {
				convey.So(returnedURL, convey.ShouldEqual, codeListAPIURL)
			})
		})
	})
}

func TestBuilder_GetImportAPIURL(t *testing.T) {
	convey.Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)

		convey.Convey("When GetImportAPIURL is called", func() {
			returnedURL := urlBuilder.GetImportAPIURL()

			convey.Convey("Then the expected URL is returned", func() {
				convey.So(returnedURL, convey.ShouldEqual, importAPIURL)
			})
		})
	})
}
