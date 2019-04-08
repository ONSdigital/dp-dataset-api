package models

import (
	"net/url"
)

const (
	codeListURI = "/code-lists/%s"
	editionsURI = "/code-lists/%s/editions"
	editionURI  = "/code-lists/%s/editions/%s"
	codesURI    = "/code-lists/%s/editions/%s/codes"
	codeURI     = "/code-lists/%s/editions/%s/codes/%s"
	datasetsURI = "/code-lists/%s/editions/%s/codes/%s/datasets"
)

// Link contains the id and a link to a resource
type Link struct {
	ID   string `json:"id,omitempty"     bson:"id"`
	Href string `json:"href"             bson:"href"`
}

func CreateLink(id, href, host string) *Link {

	rel, err := url.Parse(href)
	if err != nil {
		return nil
	}

	d, err := url.Parse(host)
	if err != nil {
		return nil
	}

	//if the configured host contains a path persist it
	d.Path = d.Path + rel.Path

	return &Link{
		ID:   id,
		Href: d.String(),
	}
}
