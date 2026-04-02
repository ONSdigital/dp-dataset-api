package utils

// GeneratePurgePrefixes generates a list of URL prefixes to send for cache purging.
// It includes prefixes for both the website and API public URLs.
func GeneratePurgePrefixes(websiteURL, apiRouterPublicURL, topicSlug, datasetID, edition, version string) []string {
	return []string{
		websiteURL + "/" + topicSlug + "/datasets/" + datasetID,
		websiteURL + "/" + topicSlug + "/datasets/" + datasetID + "/editions",
		websiteURL + "/" + topicSlug + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
		apiRouterPublicURL + "/datasets/" + datasetID,
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions",
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
	}
}
