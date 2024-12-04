package url_test

import (
	"fmt"
	neturl "net/url"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	websiteURL = "localhost:20000"
	datasetID  = "123"
	edition    = "2017"
	version    = "1"
)

func TestBuilder_BuildWebsiteDatasetVersionURL(t *testing.T) {
	Convey("Given a URL builder", t, func() {

		websiteURLparsed, _ := neturl.Parse("localhost:20000")
		urlBuilder := url.NewBuilder(websiteURLparsed)

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
