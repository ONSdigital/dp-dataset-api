package utils

// GeneratePurgePrefixes generates a list of URL prefixes to send for cache purging
// It includes prefixes for both the website and API public URLs
func GeneratePurgePrefixes(websiteURL, apiRouterPublicURL, datasetID, edition, version string) []string {
	return []string{
		websiteURL + "/datasets/" + datasetID,
		websiteURL + "/datasets/" + datasetID + "/editions",
		websiteURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
		apiRouterPublicURL + "/datasets/" + datasetID,
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions",
		apiRouterPublicURL + "/datasets/" + datasetID + "/editions/" + edition + "/versions",
	}
}
