package models

import (
	"fmt"
)

const codelistFormat = "%s/code-lists/%s/codes"
const rootFormat = "%s/hierarchies/%s/%s"

// CodelistURL set by main() to make accessible to all models users
var CodelistURL string

// Response models a node in the hierarchy
type Response struct {
	ID           string          `json:"-"`
	Label        string          `json:"label"`
	Children     []*Element      `json:"children,omitempty"`
	NoOfChildren int64           `json:"no_of_children,omitempty"`
	Links        map[string]Link `json:"links,omitempty"`
	HasData      bool            `json:"has_data"`
	Breadcrumbs  []*Element      `json:"breadcrumbs,omitempty"`
}

// Element is a item in a list within a Response
type Element struct {
	ID           string          `json:"-"`
	Label        string          `json:"label"`
	NoOfChildren int64           `json:"no_of_children,omitempty"`
	Links        map[string]Link `json:"links,omitempty"`
	HasData      bool            `json:"has_data"`
}

// Link is a combination of ID and HRef for the object in question
type Link struct {
	ID   string `json:"id,omitempty"`
	HRef string `json:"href,omitempty"`
}

// AddLinks adds links (self, codelist and populates children links)
func (r *Response) AddLinks(host, instanceID, dimensionName, codelistID string, isRoot bool) {
	if r.Links == nil {
		r.Links = make(map[string]Link)
	}

	if isRoot {
		r.Links["self"] = *GetLink(fmt.Sprintf(rootFormat, host, instanceID, dimensionName), "")
	} else {
		r.Links["self"] = *GetLinkWithID(fmt.Sprintf(rootFormat, host, instanceID, dimensionName), r.ID, r.ID)
	}

	r.Links["code"] = *GetLinkWithID(fmt.Sprintf(codelistFormat, CodelistURL, codelistID), r.ID, r.ID)

	for _, child := range r.Children {
		child.AddLinks(host, instanceID, dimensionName, codelistID, true)
	}

	an := len(r.Breadcrumbs)
	for i, crumb := range r.Breadcrumbs {

		withID := true
		if i == (an - 1) {
			withID = false
		}

		crumb.AddLinks(host, instanceID, dimensionName, codelistID, withID)
	}
}

// AddLinks adds self and codelist links for Elements
func (e *Element) AddLinks(host, instanceID, dimensionName, codelistID string, withID bool) {
	if e.Links == nil {
		e.Links = make(map[string]Link)
	}

	if !withID {
		e.Links["self"] = *GetLink(fmt.Sprintf(rootFormat, host, instanceID, dimensionName), "")
	} else {
		e.Links["self"] = *GetLinkWithID(fmt.Sprintf(rootFormat, host, instanceID, dimensionName), e.ID, e.ID)
	}

	e.Links["code"] = *GetLinkWithID(fmt.Sprintf(codelistFormat, CodelistURL, codelistID), e.ID, e.ID)
}

// GetLink returns a Link{id,href} object for the given url/id (or just url if id is empty)
func GetLink(baseURL string, linkID string) *Link {
	if linkID == "" {
		return &Link{HRef: baseURL}
	}
	return &Link{HRef: baseURL + "/" + linkID}
}

// GetLinkWithID returns a Link{id,href} object for the given url/id (or just url if id is empty)
func GetLinkWithID(baseURL string, linkID, id string) *Link {
	if linkID == "" {
		return &Link{HRef: baseURL, ID: id}
	}
	return &Link{HRef: baseURL + "/" + linkID, ID: id}
}
