package url_test

import (
	"fmt"
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
		urlBuilder := url.NewBuilder(websiteURL)

		Convey("When BuildWebsiteDatasetVersionURL is called", func() {

			builtUrl := urlBuilder.BuildWebsiteDatasetVersionURL(datasetID, edition, version)

			expectedURL := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
				websiteURL, datasetID, edition, version)

			Convey("Then the expected URL is returned", func() {
				So(builtUrl, ShouldEqual, expectedURL)
			})
		})
	})
}
