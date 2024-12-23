package url

import (
	"fmt"
	"net/url"
)

// Builder encapsulates the building of urls in a central place, with knowledge of the url structures and base host names.
type Builder struct {
	websiteURL         *url.URL
	downloadServiceURL *url.URL
	datasetAPIURL      *url.URL
}

// NewBuilder returns a new instance of url.Builder
func NewBuilder(websiteURL *url.URL, downloadServiceURL *url.URL, datasetAPIURL *url.URL) *Builder {
	return &Builder{
		websiteURL:         websiteURL,
		downloadServiceURL: downloadServiceURL,
		datasetAPIURL:      datasetAPIURL,
	}
}

func (builder *Builder) GetWebsiteURL() *url.URL {
	return builder.websiteURL
}

func (builder *Builder) GetDownloadServiceURL() *url.URL {
	return builder.downloadServiceURL
}

func (builder *Builder) GetDatasetAPIURL() *url.URL {
	return builder.datasetAPIURL
}

// BuildWebsiteDatasetVersionURL returns the website URL for a specific dataset version
func (builder Builder) BuildWebsiteDatasetVersionURL(datasetID, edition, version string) string {
	return fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
		builder.websiteURL.String(), datasetID, edition, version)
}
