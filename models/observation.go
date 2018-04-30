package models

// ObservationDoc represents information (metadata) relevant to a version
type ObservationDoc struct {
	Dimensions          map[string]Options `json:"dimensions,omitempty"`
	Context             string             `json:"context,omitempty"`
	Links               *ObservationLinks  `json:"links,omitempty"`
	Observation         string             `json:"observation,omitempty"`
	ObservationMetadata map[string]string  `json:"observation_level_metadata,omitempty"`
	UsageNotes          *[]UsageNote       `json:"usage_notes,omitempty"`
}

// ObservationLinks represents a link object to list of links relevant to the observation
type ObservationLinks struct {
	DatasetMetadata *LinkObject `json:"dataset_metadata,omitempty"`
	Self            *LinkObject `json:"self,omitempty"`
	Version         *LinkObject `json:"version,omitempty"`
}

// Options is a an object containing a list of link onjects that refer to the
// code url for that dimension option
type Options struct {
	LinkObjects []*LinkObject `json:"options,omitempty"`
}

// CreateObservationDoc manages the creation of metadata across dataset and version docs
func CreateObservationDoc(versionDoc *Version, headerRow, observationRow []string, dimensionOffset int, queryParameters map[string]string) *ObservationDoc {

	observationDoc := &ObservationDoc{
		Context: "",
		Links: &ObservationLinks{
			DatasetMetadata: &LinkObject{
				HRef: versionDoc.Links.Version.HRef + "/metadata",
			},
			Self: &LinkObject{
				HRef: versionDoc.Links.Version.HRef + "/observations",
			},
			Version: &LinkObject{
				HRef: versionDoc.Links.Version.HRef,
				ID:   versionDoc.Links.Version.ID,
			},
		},
		Observation: observationRow[0],
		UsageNotes:  versionDoc.UsageNotes,
	}

	// add observation metadata
	if dimensionOffset != 0 {
		observationMetaData := make(map[string]string)

		for i := 1; i < dimensionOffset+1; i++ {
			observationMetaData[headerRow[i]] = observationRow[i]
		}

		observationDoc.ObservationMetadata = observationMetaData
	}

	var dimensions = make(map[string]Options)

	// add the dimension codes
	for paramKey, paramValue := range queryParameters {
		for _, dimension := range versionDoc.Dimensions {

			var linkObjects []*LinkObject
			if dimension.Name == paramKey {

				linkObject := &LinkObject{
					HRef: dimension.HRef + "/codes/" + paramValue,
					ID:   paramValue,
				}
				linkObjects = append(linkObjects, linkObject)
				dimensions[paramKey] = Options{
					LinkObjects: linkObjects,
				}
				break
			}
		}
	}
	observationDoc.Dimensions = dimensions

	return observationDoc
}
