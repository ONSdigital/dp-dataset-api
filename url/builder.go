package url

import (
	"net/url"
)

// Builder encapsulates the building of urls in a central place, with knowledge of the url structures and base host names.
type Builder struct {
	publicWebsiteURL   *url.URL
	privateWebsiteURL  *url.URL
	downloadServiceURL *url.URL
	datasetAPIURL      *url.URL
	codeListAPIURL     *url.URL
	importAPIURL       *url.URL
	apiRouterPublicURL *url.URL
}

// NewBuilder returns a new instance of url.Builder
func NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL *url.URL) *Builder {
	return &Builder{
		publicWebsiteURL:   publicWebsiteURL,
		privateWebsiteURL:  privateWebsiteURL,
		downloadServiceURL: downloadServiceURL,
		datasetAPIURL:      datasetAPIURL,
		codeListAPIURL:     codeListAPIURL,
		importAPIURL:       importAPIURL,
		apiRouterPublicURL: apiRouterPublicURL,
	}
}

func (builder *Builder) GetPublicWebsiteURL() *url.URL {
	return builder.publicWebsiteURL
}

func (builder *Builder) GetPrivateWebsiteURL() *url.URL {
	return builder.privateWebsiteURL
}

func (builder *Builder) GetDownloadServiceURL() *url.URL {
	return builder.downloadServiceURL
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

func (builder *Builder) GetAPIRouterPublicURL() *url.URL {
	return builder.apiRouterPublicURL
}
