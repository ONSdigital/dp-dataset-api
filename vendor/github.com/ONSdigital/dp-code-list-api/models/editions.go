package models

import (
	"errors"
	"fmt"
)

// Editions represents the editions response model
type Editions struct {
	Items      []Edition `json:"items"`
	Count      int       `json:"count"`
	Offset     int       `json:"offset"`
	Limit      int       `json:"limit"`
	TotalCount int       `json:"total_count"`
}

// Edition represents a single edition response model
type Edition struct {
	Edition string        `json:"edition"`
	Label   string        `json:"label"`
	Links   *EditionLinks `json:"links"`
}

// EditionLinks reprsents the links returned for a specific edition
type EditionLinks struct {
	Self     *Link `json:"self"`
	Editions *Link `json:"editions"`
	Codes    *Link `json:"codes"`
}

func (e *Edition) UpdateLinks(codeListID, url string) error {
	if e.Links == nil || e.Links.Self == nil || e.Links.Self.ID == "" {
		return errors.New("unable to create links - edition id not provided")
	}

	id := e.Links.Self.ID
	e.Links.Self = CreateLink(id, fmt.Sprintf(editionURI, codeListID, id), url)
	e.Links.Editions = CreateLink("", fmt.Sprintf(editionsURI, codeListID), url)
	e.Links.Codes = CreateLink("", fmt.Sprintf(codesURI, codeListID, id), url)

	return nil
}
