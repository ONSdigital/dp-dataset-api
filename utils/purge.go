package utils

import "strings"

// GeneratePurgePrefixes generates a list of URL prefixes to send for cache purging.
// It includes prefixes for both the website and API public URLs.
func GeneratePurgePrefixes(publicWebsiteURL, apiRouterPublicURL, topicSlug, datasetID, edition, version string) []string {
	// Remove the scheme as Cloudflare expects URLs without it for cache purging.
	publicWebsiteURL = removeSchemeFromURL(publicWebsiteURL)
	apiRouterPublicURL = removeSchemeFromURL(apiRouterPublicURL)

	return []string{
		publicWebsiteURL + "/" + topicSlug + "/datasets/" + datasetID,
		publicWebsiteURL + "/" + topicSlug + "/datasets/" + datasetID + "/editions",
		publicWebsiteURL + "/" + topicSlug + "/datasets/" + datasetID + "/editions/" + edition,
		publicWebsiteURL + "/" + topicSlug + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
		publicWebsiteURL + "/" + topicSlug + "/datasets/" + datasetID + "/editions/" + edition + "/versions/" + version,
		apiRouterPublicURL + "/datasets/" + datasetID,
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions",
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions/" + edition,
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/" + version,
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions/" + version + "/metadata",
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
