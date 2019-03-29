package model

import (
	"errors"
	"fmt"
	"strings"
)

// DimensionNodeResults wraps dimension node objects for pagination
type DimensionNodeResults struct {
	Items []*Dimension `json:"items"`
}

// Dimension struct encapsulating Dimension details.
type Dimension struct {
	DimensionID string        `json:"dimension"`
	Option      string        `json:"option"`
	NodeID      string        `json:"node_id,omitempty"`
	Links       Links         `json:"links"`
	Dimensions  []interface{} `json:"-"`
}

type Links struct {
	CodeList Link `json:"code_list,omitempty"`
	Code     Link `json:"code,omitempty"`
}

// Link represents a single link within a dataset model
type Link struct {
	HRef string `json:"href"`
	ID   string `json:"id,omitempty"`
}

// GetName return the name or type of Dimension e.g. sex, geography time etc.
func (d *Dimension) GetName(instanceID string) string {
	instID := fmt.Sprintf("_%s_", instanceID)
	dimLabel := "_" + d.DimensionID
	result := strings.Replace(dimLabel, instID, "", 2)
	return result
}

func (d *Dimension) Validate() error {
	if d == nil {
		return errors.New("dimension is required but was nil")
	}
	if len(d.DimensionID) == 0 && len(d.Option) == 0 {
		return errors.New("dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty")
	}
	if len(d.DimensionID) == 0 {
		return errors.New("dimension id is required but was empty")
	}
	if len(d.Option) == 0 {
		return errors.New("dimension value is required but was empty")
	}
	return nil
}

// Instance struct to hold instance information.
type Instance struct {
	InstanceID string        `json:"id,omitempty"`
	CSVHeader  []string      `json:"headers"`
	Dimensions []interface{} `json:"-"`
}

// AddDimension add a dimension distinct type/name to the instance.
func (i *Instance) AddDimension(d *Dimension) {
	i.Dimensions = append(i.Dimensions, string(d.DimensionID))
}

func (i *Instance) Validate() error {
	if i == nil {
		return errors.New("instance is required but was nil")
	}
	if len(i.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}
	return nil
}
