package models

type Page struct {
	Items      interface{} `json:"items"`
	Count      int         `json:"count"`
	Offset     int         `json:"offset"`
	Limit      int         `json:"limit"`
	TotalCount int         `json:"total_count"`
}

type PageLinks struct {
	Next *LinkObject `json:"next,omitempty"`
	Prev *LinkObject `json:"prev,omitempty"`
	Self *LinkObject `json:"self,omitempty"`
}

// LinkObject represents a generic structure for all links
type LinkObject struct {
	HRef string `bson:"href,omitempty"  json:"href,omitempty"`
	ID   string `bson:"id,omitempty"    json:"id,omitempty"`
}

// LDDatasetLinks ...
type LDDatasetLinks struct { //TODO - can potentially remove bson tags here as its "-" in the only usage
	Editions          *LinkObject `bson:"-"        json:"editions,omitempty" groups:"dataset"`
	Self              *LinkObject `bson:"-"        json:"self,omitempty" groups:"datasets,dataset"`
	LatestVersionLink `bson:",inline"`
}

// EditionLinks ...
type EditionLinks struct {
	Editions      *LinkObject `bson:"-" json:"editions,omitempty" groups:"edition"`
	Versions      *LinkObject `bson:"-" json:"versions,omitempty" groups:"edition"`
	Dimensions    *LinkObject `bson:"-" json:"dimensions,omitempty" groups:"versions,version"`
	Distributions *LinkObject `bson:"-" json:"distributions,omitempty" groups:"versions,version"`
	//LatestVersion *LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty" groups:"edition"`
	Next              *LinkObject `bson:"next,omitempty"        json:"next,omitempty" groups:"edition,version"`
	Prev              *LinkObject `bson:"prev,omitempty"        json:"prev,omitempty" groups:"edition,version"`
	DatasetLink       `bson:",inline"`
	EditionLink       `bson:",inline"`
	VersionLink       `bson:",inline"`
	SelfLink          `bson:",inline"`
	LatestVersionLink `bson:",inline"`
}

// InstanceLinks holds all links for an instance
type LDInstanceLinks struct {
	Job          *LinkObject `bson:"job,omitempty"        json:"job,omitempty" groups:"instances,instance"`
	EditionLinks `bson:",inline"`
}

type LDDimensionLinks struct {
	Instance    *LinkObject `bson:"instance,omitempty" json:"instance,omitempty"`
	SelfLink    `bson:",inline"`
	DatasetLink `bson:",inline"`
	EditionLink `bson:",inline"`
	VersionLink `bson:",inline"`
}

type DatasetLink struct {
	Dataset *LinkObject `bson:"dataset,omitempty" json:"dataset,omitempty" groups:"edition,version,instances,instance,dimensions,distributions"`
}

type EditionLink struct {
	Edition *LinkObject `bson:"edition,omitempty" json:"edition,omitempty" groups:"version,instances,instance,dimensions,distributions"`
}

type VersionLink struct {
	Version *LinkObject `bson:"version,omitempty" json:"version,omitempty" groups:"instances,instance,dimensions,distributions"`
}

type SelfLink struct {
	Self *LinkObject `bson:"self,omitempty" json:"self,omitempty" groups:"all"`
}

type LatestVersionLink struct {
	LatestVersion *LinkObject `bson:"latest_version,omitempty"  json:"latest_version,omitempty" groups:"datasets,dataset,edition"`
}
