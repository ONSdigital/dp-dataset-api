package models

const wildcard = "*"

// ObservationsDoc represents information (observations) relevant to a version
type ObservationsDoc struct {
	Dimensions        map[string]Option `json:"dimensions"`
	Limit             int               `json:"limit"`
	Links             *ObservationLinks `json:"links"`
	Observations      []Observation     `json:"observations"`
	Offset            int               `json:"offset"`
	TotalObservations int               `json:"total_observations"`
	UnitOfMeasure     string            `json:"unit_of_measure,omitempty"`
	UsageNotes        *[]UsageNote      `json:"usage_notes,omitempty"`
}

// Observation represents an object containing a single
// observation and its equivalent metadata
type Observation struct {
	Dimensions  map[string]*DimensionObject `json:"dimensions,omitempty"`
	Metadata    map[string]string           `json:"metadata,omitempty"`
	Observation string                      `json:"observation"`
}

// DimensionObject represents ...
type DimensionObject struct {
	HRef  string `json:"href"`
	ID    string `json:"id"`
	Label string `json:"label"`
}

// ObservationLinks represents a link object to list of links relevant to the observation
type ObservationLinks struct {
	DatasetMetadata *LinkObject `json:"dataset_metadata,omitempty"`
	Self            *LinkObject `json:"self,omitempty"`
	Version         *LinkObject `json:"version,omitempty"`
}

// Option represents an object containing a list of link objects that refer to the
// code url for that dimension option
type Option struct {
	LinkObject *LinkObject `json:"option,omitempty"`
}

// CreateObservationsDoc manages the creation of metadata across dataset and version docs
func CreateObservationsDoc(rawQuery string, versionDoc *Version, datasetDoc *Dataset, observations []Observation, queryParameters map[string]string, offset, limit int) *ObservationsDoc {

	observationsDoc := &ObservationsDoc{
		Limit: limit,
		Links: &ObservationLinks{
			DatasetMetadata: &LinkObject{
				HRef: versionDoc.Links.Version.HRef + "/metadata",
			},
			Self: &LinkObject{
				HRef: versionDoc.Links.Version.HRef + "/observations?" + rawQuery,
			},
			Version: &LinkObject{
				HRef: versionDoc.Links.Version.HRef,
				ID:   versionDoc.Links.Version.ID,
			},
		},
		Observations:      observations,
		Offset:            offset,
		TotalObservations: len(observations),
		UnitOfMeasure:     datasetDoc.UnitOfMeasure,
		UsageNotes:        versionDoc.UsageNotes,
	}

	var dimensions = make(map[string]Option)

	// add the dimension codes
	for paramKey, paramValue := range queryParameters {
		for _, dimension := range versionDoc.Dimensions {
			var linkObjects []*LinkObject
			if dimension.Name == paramKey && paramValue != wildcard {

				linkObject := &LinkObject{
					HRef: dimension.HRef + "/codes/" + paramValue,
					ID:   paramValue,
				}
				linkObjects = append(linkObjects, linkObject)
				dimensions[paramKey] = Option{
					LinkObject: linkObject,
				}
				break
			}
		}
	}
	observationsDoc.Dimensions = dimensions

	return observationsDoc
}
