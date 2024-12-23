package url_test

import (
	"fmt"
	neturl "net/url"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	websiteURL = "http://localhost:20000"
	datasetID  = "123"
	edition    = "2017"
	version    = "1"
)

func TestBuilder_BuildWebsiteDatasetVersionURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {

		websiteURL, _ := neturl.Parse("http://localhost:20000")
		downloadServiceURL, _ := neturl.Parse("http://localhost:23600")
		datasetAPIURL, _ := neturl.Parse("http://localhost:22000")
		urlBuilder := url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL)

		Convey("When BuildWebsiteDatasetVersionURL is called", func() {
			builtURL := urlBuilder.BuildWebsiteDatasetVersionURL(datasetID, edition, version)

			expectedURL := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
				websiteURL, datasetID, edition, version)

			Convey("Then the expected URL is returned", func() {
				So(builtURL, ShouldEqual, expectedURL)
			})
		})
	})
}
