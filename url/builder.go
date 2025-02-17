package url

import (
	"fmt"
	"net/url"
)

// Builder encapsulates the building of urls in a central place, with knowledge of the url structures and base host names.
type Builder struct {
	websiteURL                 *url.URL
	downloadServiceURL         *url.URL
	externalDownloadServiceURL *url.URL
	datasetAPIURL              *url.URL
	codeListAPIURL             *url.URL
	importAPIURL               *url.URL
}

// NewBuilder returns a new instance of url.Builder
func NewBuilder(websiteURL, downloadServiceURL, externalDownloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL *url.URL) *Builder {
	return &Builder{
		websiteURL:                 websiteURL,
		downloadServiceURL:         downloadServiceURL,
		externalDownloadServiceURL: externalDownloadServiceURL,
		datasetAPIURL:              datasetAPIURL,
		codeListAPIURL:             codeListAPIURL,
		importAPIURL:               importAPIURL,
	}
}

func (builder *Builder) GetWebsiteURL() *url.URL {
	return builder.websiteURL
}

func (builder *Builder) GetDownloadServiceURL() *url.URL {
	return builder.downloadServiceURL
}

func (builder *Builder) GetExternalDownloadServiceURL() *url.URL {
	return builder.externalDownloadServiceURL
}

func (builder *Builder) GetDatasetAPIURL() *url.URL {
	return builder.datasetAPIURL
}

func (builder *Builder) GetCodeListAPIURL() *url.URL {
	return builder.codeListAPIURL
}

func (builder *Builder) GetImportAPIURL() *url.URL {
	return builder.importAPIURL
}

// BuildWebsiteDatasetVersionURL returns the website URL for a specific dataset version
func (builder Builder) BuildWebsiteDatasetVersionURL(datasetID, edition, version string) string {
	return fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
		builder.websiteURL.String(), datasetID, edition, version)
}
