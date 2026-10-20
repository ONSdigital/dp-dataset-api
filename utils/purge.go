package utils

import "strings"

// GeneratePurgePrefixes generates a list of URL prefixes to send for cache purging.
// It includes prefixes for both the website and API public URLs.
// Topic slug is optional, and if provided, it will be prefixed to the website path.
func GeneratePurgePrefixes(publicWebsiteURL, apiRouterPublicURL, datasetID, topicSlug string) []string {
	// Remove the scheme as Cloudflare expects URLs without it for cache purging.
	publicWebsiteURL = removeSchemeFromURL(publicWebsiteURL)
	apiRouterPublicURL = removeSchemeFromURL(apiRouterPublicURL)

	websitePath := "/datasets/" + datasetID
	if topicSlug != "" {
		websitePath = "/" + topicSlug + websitePath
	}

	return []string{
		publicWebsiteURL + websitePath,
		apiRouterPublicURL + "/datasets/" + datasetID,
	}
}

// removeSchemeFromURL removes the "https://" or "http://" scheme from a URL if it exists.
func removeSchemeFromURL(rawURL string) string {
	if urlWithoutScheme, ok := strings.CutPrefix(rawURL, "https://"); ok {
		return urlWithoutScheme
	}
	if urlWithoutScheme, ok := strings.CutPrefix(rawURL, "http://"); ok {
		return urlWithoutScheme
	}
	return rawURL
}
