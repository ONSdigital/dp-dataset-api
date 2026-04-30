package url_test

import (
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
	publicWebsiteURL   = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	privateWebsiteURL  = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	apiRouterPublicURL = &neturl.URL{Scheme: "http", Host: "localhost:23200", Path: "v1"}
)

func TestBuilder_GetPublicWebsiteURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetPublicWebsiteURL is called", func() {
			returnedURL := urlBuilder.GetPublicWebsiteURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, publicWebsiteURL)
			})
		})
	})
}

func TestBuilder_GetDownloadServiceURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

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
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

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
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

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
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

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
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetAPIRouterPublicURL is called", func() {
			returnedURL := urlBuilder.GetAPIRouterPublicURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, apiRouterPublicURL)
			})
		})
	})
}

func TestBuilder_GetPrivateWebsiteURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {
		urlBuilder := url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)

		Convey("When GetPrivateWebsiteURL is called", func() {
			returnedURL := urlBuilder.GetPrivateWebsiteURL()

			Convey("Then the expected URL is returned", func() {
				So(returnedURL, ShouldEqual, privateWebsiteURL)
			})
		})
	})
}
