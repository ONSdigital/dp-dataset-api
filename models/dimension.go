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
	Links       DimensionLink `bson:"links,omitempty"         json:"links,omitempty"`
	Name        string        `bson:"name,omitempty"          json:"dimension,omitempty"`
	LastUpdated time.Time     `bson:"last_updated,omitempty"  json:"-"`
	Label       string        `bson:"label,omitempty"         json:"label,omitempty"`
}

// DimensionLink contains all links needed for a dimension
type DimensionLink struct {
	CodeList LinkObject `bson:"code_list,omitempty"     json:"code_list,omitempty"`
	Options  LinkObject `bson:"options,omitempty"       json:"options,omitempty"`
	Version  LinkObject `bson:"version,omitempty"       json:"version,omitempty"`
}

// CachedDimensionOption contains information used to create a dimension option
type CachedDimensionOption struct {
	Name       string `bson:"name,omitempty"           json:"dimension"`
	Code       string `bson:"code,omitempty"           json:"code"`
	NodeID     string `bson:"node_id,omitempty"        json:"node_id"`
	InstanceID string `bson:"instance_id,omitempty"    json:"instance_id,omitempty"`
	CodeList   string `bson:"code_list,omitempty"      json:"code_list,omitempty"`
	Option     string `bson:"option,omitempty"         json:"option"`
	Label      string `bson:"label,omitempty"          json:"label"`
}

// DimensionOption contains unique information and metadata used when processing the data
type DimensionOption struct {
	Name        string               `bson:"name,omitempty"           json:"dimension"`
	Label       string               `bson:"label,omitempty"          json:"label"`
	Links       DimensionOptionLinks `bson:"links,omitempty"          json:"links"`
	Option      string               `bson:"option,omitempty"         json:"option"`
	NodeID      string               `bson:"node_id,omitempty"        json:"node_id"`
	InstanceID  string               `bson:"instance_id,omitempty"    json:"instance_id,omitempty"`
	LastUpdated time.Time            `bson:"last_updated,omitempty"   json:"-"`
}

// PublicDimensionOption hides values which are only used by interval services
type PublicDimensionOption struct {
	Name   string               `bson:"name,omitempty"           json:"dimension"`
	Label  string               `bson:"label,omitempty"          json:"label"`
	Links  DimensionOptionLinks `bson:"links,omitempty"          json:"links"`
	Option string               `bson:"option,omitempty"         json:"option"`
}

// DimensionOptionLinks represents a list of link objects related to dimension options
type DimensionOptionLinks struct {
	Code     LinkObject `bson:"code,omitempty"              json:"code"`
	Version  LinkObject `bson:"version,omitempty"           json:"version"`
	CodeList LinkObject `bson:"code_list,omitempty"         json:"code_list"`
}

// DimensionNodeResults wraps dimension node objects for pagination
type DimensionNodeResults struct {
	Items []DimensionOption `json:"items"`
}

// DimensionValues holds all unique values for a dimension
type DimensionValues struct {
	Name   string   `json:"dimension"`
	Values []string `json:"values"`
}
