package models

import (
	"errors"
	"fmt"
)

// CodeResults contains an array of codes which can be paginated
type CodeResults struct {
	Items      []Code `json:"items"`
	Count      int    `json:"count"`
	Offset     int    `json:"offset"`
	Limit      int    `json:"limit"`
	TotalCount int    `json:"total_count"`
}

// Code for a single dimensions type
type Code struct {
	ID    string     `json:"-"`
	Code  string     `json:"id"`
	Label string     `json:"label"`
	Links *CodeLinks `json:"links"`
}

// CodeLinks contains links for a code resource
type CodeLinks struct {
	CodeList *Link `json:"code_list"`
	Datasets *Link `json:"datasets"`
	Self     *Link `json:"self"`
}

func (c *Code) UpdateLinks(host, codeListID, edition string) error {
	if c.Links == nil || c.Links.Self == nil || c.Links.Self.ID == "" {
		return errors.New("unable to create links - code id not provided")
	}

	id := c.Links.Self.ID
	c.Links.Self = CreateLink(id, fmt.Sprintf(codeURI, codeListID, edition, id), host)
	c.Links.Datasets = CreateLink("", fmt.Sprintf(datasetsURI, codeListID, edition, id), host)
	c.Links.CodeList = CreateLink("", fmt.Sprintf(codeListURI, codeListID), host)

	return nil
}
