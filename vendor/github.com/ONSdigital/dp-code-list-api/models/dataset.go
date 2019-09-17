package models

import (
	"errors"
	"fmt"
)

// Datasets represents the model returned from the api datasets
// endpoint
type Datasets struct {
	Items      []Dataset `json:"items"`
	Count      int       `json:"count"`
	Offset     int       `json:"offset"`
	Limit      int       `json:"limit"`
	TotalCount int       `json:"total_count"`
}

// Dataset represents an individual model dataset
type Dataset struct {
	Links          *DatasetLinks    `json:"links"`
	DimensionLabel string           `json:"dimension_label"`
	Editions       []DatasetEdition `json:"editions"`
}

type DatasetEdition struct {
	Links *DatasetEditionLinks `json:"links"`
}

type DatasetEditionLinks struct {
	Self             *Link `json:"self"`
	DatasetDimension *Link `json:"dataset_dimension"`
	LatestVersion    *Link `json:"latest_version"`
}

// DatasetLink represents the links returned specifically for a dataset
type DatasetLinks struct {
	Self *Link `json:"self"`
}

func (ds *Datasets) UpdateLinks(host, datasetAPIhost, codeListID, editionID, codeID string) error {
	for i, dataset := range ds.Items {
		if dataset.Links == nil || dataset.Links.Self == nil || dataset.Links.Self.ID == "" {
			return errors.New("invalid dataset provided")
		}

		id := dataset.Links.Self.ID
		l := CreateLink(id, fmt.Sprintf(datasetAPIuri, id), datasetAPIhost)
		dataset.Links.Self = &Link{
			Href: l.Href,
			ID:   id,
		}

		var editions []DatasetEdition
		for _, edition := range dataset.Editions {
			if edition.Links == nil || edition.Links.Self == nil || edition.Links.Self.ID == "" {
				continue
			}

			editionID := edition.Links.Self.ID
			edition.Links.Self = CreateLink(editionID, fmt.Sprintf("/datasets/%s/editions/%s", id, editionID), datasetAPIhost)
			//	latestVersion := strconv.Itoa(max(versions))

			if edition.Links == nil || edition.Links.LatestVersion == nil || edition.Links.LatestVersion.ID == "" {
				continue
			}

			versionID := edition.Links.LatestVersion.ID
			edition.Links.LatestVersion = CreateLink(versionID, fmt.Sprintf("/datasets/%s/editions/%s/versions/%s", id, editionID, versionID), datasetAPIhost)

			if edition.Links == nil || edition.Links.DatasetDimension == nil || edition.Links.DatasetDimension.ID == "" {
				continue
			}

			dimensionID := edition.Links.DatasetDimension.ID
			edition.Links.DatasetDimension = CreateLink(dimensionID, fmt.Sprintf("/datasets/%s/editions/%s/versions/%s/dimensions/%s", id, editionID, versionID, dimensionID), datasetAPIhost)

			editions = append(editions, edition)
		}

		dataset.Editions = editions
		ds.Items[i] = dataset
	}
	return nil
}
