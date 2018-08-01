package models

import "time"

// DatasetDimensionResults represents a structure for a list of dimensions
type DatasetDimensionResults struct {
	Items []Dimension `json:"items"`
}

// DimensionOptionResults represents a structure for a list of dimension options
type DimensionOptionResults struct {
	Items []PublicDimensionOption `json:"items"`
}

// Dimension represents an overview for a single dimension. This includes a link to the code list API
// which provides metadata about the dimension and all possible values.
type Dimension struct {
	Description string        `bson:"description,omitempty"   json:"description,omitempty"`
	Label       string        `bson:"label,omitempty"         json:"label,omitempty"`
	LastUpdated time.Time     `bson:"last_updated,omitempty"  json:"-"`
	Links       DimensionLink `bson:"links,omitempty"         json:"links,omitempty"`
	HRef        string        `json:"href,omitempty"`
	ID          string        `json:"id,omitempty"`
	Name        string        `bson:"name,omitempty"          json:"name,omitempty"`
}

// DimensionLink contains all links needed for a dimension
type DimensionLink struct {
	CodeList LinkObject `bson:"code_list,omitempty"     json:"code_list,omitempty"`
	Options  LinkObject `bson:"options,omitempty"       json:"options,omitempty"`
	Version  LinkObject `bson:"version,omitempty"       json:"version,omitempty"`
}

// CachedDimensionOption contains information used to create a dimension option
type CachedDimensionOption struct {
	Code       string `bson:"code,omitempty"           json:"code"`
	CodeList   string `bson:"code_list,omitempty"      json:"code_list,omitempty"`
	InstanceID string `bson:"instance_id,omitempty"    json:"instance_id,omitempty"`
	Label      string `bson:"label,omitempty"          json:"label"`
	Name       string `bson:"name,omitempty"           json:"dimension"`
	NodeID     string `bson:"node_id,omitempty"        json:"node_id"`
	Option     string `bson:"option,omitempty"         json:"option"`
}

// DimensionOption contains unique information and metadata used when processing the data
type DimensionOption struct {
	InstanceID  string               `bson:"instance_id,omitempty"    json:"instance_id,omitempty"`
	Label       string               `bson:"label,omitempty"          json:"label"`
	LastUpdated time.Time            `bson:"last_updated,omitempty"   json:"-"`
	Links       DimensionOptionLinks `bson:"links,omitempty"          json:"links"`
	Name        string               `bson:"name,omitempty"           json:"dimension"`
	NodeID      string               `bson:"node_id,omitempty"        json:"node_id"`
	Option      string               `bson:"option,omitempty"         json:"option"`
}

// PublicDimensionOption hides values which are only used by interval services
type PublicDimensionOption struct {
	Label  string               `bson:"label,omitempty"          json:"label"`
	Links  DimensionOptionLinks `bson:"links,omitempty"          json:"links"`
	Name   string               `bson:"name,omitempty"           json:"dimension"`
	Option string               `bson:"option,omitempty"         json:"option"`
}

// DimensionOptionLinks represents a list of link objects related to dimension options
type DimensionOptionLinks struct {
	Code     LinkObject `bson:"code,omitempty"              json:"code"`
	CodeList LinkObject `bson:"code_list,omitempty"         json:"code_list"`
	Version  LinkObject `bson:"version,omitempty"           json:"version"`
}

// DimensionNodeResults wraps dimension node objects for pagination
type DimensionNodeResults struct {
	Items []DimensionOption `json:"items"`
}

// DimensionValues holds all unique values for a dimension
type DimensionValues struct {
	Name    string   `json:"dimension"`
	Options []string `json:"options"`
}
