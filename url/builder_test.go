package url_test

import (
	"fmt"
	neturl "net/url"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	codeListAPIURL     = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL      = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL       = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	websiteURL         = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	apiRouterPublicURL = &neturl.URL{Scheme: "http", Host: "localhost:23200", Path: "v1"}
)

const (
	datasetID = "123"
	edition   = "2017"
	version   = "1"
)

func TestBuilder_BuildWebsiteDatasetVersionURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When BuildWebsiteDatasetVersionURL is called", func() {
			builtURL := urlBuilder.BuildWebsiteDatasetVersionURL(datasetID, edition, version)

			expectedURL := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
				websiteURL.String(), datasetID, edition, version)

			Convey("Then the expected URL is returned", func() {
				So(builtURL, ShouldEqual, expectedURL)
			})
		})
	})
}

func TestBuilder_GetWebsiteURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetWebsiteURL is called", func() {
			returnedURL := urlBuilder.GetWebsiteURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, websiteURL)
			})
		})
	})
}

func TestBuilder_GetDownloadServiceURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetDownloadServiceURL is called", func() {
			returnedURL := urlBuilder.GetDownloadServiceURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, downloadServiceURL)
			})
		})
	})
}

func TestBuilder_GetDatasetAPIURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetDatasetAPIURL is called", func() {
			returnedURL := urlBuilder.GetDatasetAPIURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, datasetAPIURL)
			})
		})
	})
}

func TestBuilder_GetCodeListAPIURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetCodeListAPIURL is called", func() {
			returnedURL := urlBuilder.GetCodeListAPIURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, codeListAPIURL)
			})
		})
	})
}

func TestBuilder_GetImportAPIURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetImportAPIURL is called", func() {
			returnedURL := urlBuilder.GetImportAPIURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, importAPIURL)
			})
		})
	})
}

func TestBuilder_GetAPIRouterPublicURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetAPIRouterPublicURL is called", func() {
			returnedURL := urlBuilder.GetAPIRouterPublicURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, apiRouterPublicURL)
			})
		})
	})
}
