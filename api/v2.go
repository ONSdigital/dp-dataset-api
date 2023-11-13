package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/liip/sheriff"
)

func marshal(data interface{}, opts ...string) ([]byte, error) {
	o := &sheriff.Options{
		Groups:          []string{"all"},
		IncludeEmptyTag: true,
	}

	if len(opts) > 0 {
		o.Groups = append(o.Groups, opts...)
	}

	data, err := sheriff.Marshal(o, data)
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

func (api *DatasetAPI) getV2Datasets(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)
	ctx := r.Context()

	b, err := func() ([]byte, error) {
		list, _, err := api.dataStore.Backend.GetV2Datasets(ctx, 0, 100, true)
		if err != nil {
			log.Error(ctx, "getV2Datasets endpoint: datastore.getV2Datasets returned an error", err)
			return nil, err
		}

		//linked data fields - @id per item
		for i, l := range list {
			list[i].ID = fmt.Sprintf("%s/v2/datasets/%s", api.host, l.Identifier)
		}

		// TODO: fix pagination
		page := &models.DatasetList{
			Items: list,
			Page: models.Page{
				TotalCount: len(list),
			},
			LinkedData: models.LinkedData{
				Context: "cdn.ons.gov.uk/context.json",
			},
			Links: &models.PageLinks{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets", api.host),
				},
				Next: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets", api.host),
				},
				Prev: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets", api.host),
				},
			},
		}

		groups := []string{"datasets"}
		b, err := marshal(page, groups...)
		if err != nil {
			log.Error(ctx, "getV2Datasets endpoint: marshal returned an error", err, log.Data{"groups": groups})
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "getV2Datasets endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Datasets endpoint: request successful")
}

func (api *DatasetAPI) getV2Dataset(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	log.Info(ctx, "in getV2Dataset handler")

	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetV2Dataset(ctx, true, datasetID)
		if err != nil {
			log.Error(ctx, "getV2Dataset endpoint: datastore.getV2Dataset returned an error", err)
			return nil, err
		}

		dataset.LinkedData = models.LinkedData{
			Context: "cdn.ons.gov.uk/context.json",
			ID:      fmt.Sprintf("%s/v2/datasets/%s", api.host, dataset.Identifier),
			Type:    []string{"dcat:datasetSeries"},
		}

		dataset.Links = &models.LDDatasetLinks{
			Self: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets", api.host),
			},
			Editions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, dataset.Identifier),
			},
			// TODO: query DB to get this info
			// LatestVersion: &models.LinkObject{
			// 	HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, dataset.Identifier, dataset.Lat),
			// },
		}

		// TODO: add embedded from DB queries

		// TODO set etag header?

		groups := []string{"dataset"}
		b, err := marshal(dataset, groups...)
		if err != nil {
			log.Error(ctx, "getV2Dataset endpoint: marshal returned an error", err, log.Data{"groups": groups})
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "getV2Dataset endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Dataset endpoint: request successful")
}

func (api *DatasetAPI) convertDatasets(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()

	err := func() error {
		list, _, err := api.dataStore.Backend.GetDatasets(ctx, 0, 100, true)
		if err != nil {
			log.Error(ctx, "convertDatasets endpoint: datastore.getDatasets returned an error", err)
			return err
		}

		var newList []*models.LDDataset
		for _, d := range list {
			n, err := api.ConvertDataset(ctx, d.Current, d.ID)
			if err != nil {
				log.Error(ctx, "convertDatasets endpoint: ConvertDataset returned an error", err)
				return err
			}
			newList = append(newList, n)
		}

		for i, n := range newList {
			if n == nil {
				log.Error(ctx, "convertDatasets endpoint: why are we trying to Upsert an empty new dataset?", nil, log.Data{"dataset_id": n, "index": i})
				return nil
			}

			if err := api.dataStore.Backend.UpsertLDDataset(ctx, n.Identifier, n); err != nil {
				log.Error(ctx, "convertDatasets endpoint: failed to upsert dataset document", err, log.Data{"dataset_id": n.Identifier})
				return err
			}
		}

		return nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putDataset endpoint: request successful")
}

func (api *DatasetAPI) ConvertDataset(ctx context.Context, old *models.Dataset, id string) (*models.LDDataset, error) {
	new := &models.LDDataset{
		CollectionID:   old.CollectionID,
		State:          old.State,
		CanonicalTopic: old.CanonicalTopic,
		DCATDatasetSeries: models.DCATDatasetSeries{
			Identifier:        id,
			ContactPoint:      &old.Contacts[0],
			NationalStatistic: old.NationalStatistic,
			IsBasedOn:         old.IsBasedOn,
			Survey:            old.Survey,
			Modified:          time.Now(),
			Title:             old.Title,
			Keywords:          old.Keywords,
			Themes:            []string{old.Theme},
			Description:       old.Description,
			Frequency:         old.ReleaseFrequency,
			Summary:           "This should be shorter than the description, good for search results",
			License:           "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
			NextRelease:       old.NextRelease,
		},
	}

	if old.Publisher != nil {
		new.Publisher = &models.ContactDetails{
			Name: old.Publisher.Name,
		}
	}

	// get version to populate spatial, coverage, and issued
	l, i, err := api.dataStore.Backend.GetInstances(ctx, []string{"published"}, []string{id}, 0, 500)
	if err != nil || i == 0 {
		log.Error(ctx, "convertDatasets endpoint: convertDataset GetInstances call returned an error", err, log.Data{"dataset_id": id, "list_size": i})
		return nil, err
	}

	latest := l[0]
	if old.Type == "" {
		old.Type = "filterable"
	}
	new.DatasetType = old.Type
	new.SpatialCoverage = "K04000001"

	if old.Type == "filterable" {
		new.SpatialResolution = []string{"regions", "local-authorities"}
		new.TemporalCoverage = "2002-2022"
		new.TemporalResolution = []string{"years", "months"}
	} else {
		// get version from mongo to populate this for cantab
		new.SpatialResolution = []string{latest.LowestGeography}
		new.TemporalCoverage = "2021"
		new.TemporalResolution = []string{"census"}
	}

	new.Issued, err = time.Parse(time.RFC3339, latest.ReleaseDate)
	if err != nil {
		log.Error(ctx, "convertDatasets endpoint: cannot convert release date", err)
		return nil, err
	}

	return new, nil
}
