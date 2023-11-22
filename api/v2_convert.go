package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
)

func (api *DatasetAPI) seedDB(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()

	err := func() error {
		list, _, err := api.dataStore.Backend.GetDatasets(ctx, 0, 100, true)
		if err != nil {
			log.Error(ctx, "seedDB endpoint: datastore.getDatasets returned an error", err)
			return err
		}

		var newList []*models.LDDataset
		for _, d := range list {
			n, err := api.convertDataset(ctx, d.Current, d.ID)
			if err != nil {
				log.Error(ctx, "seedDB endpoint: ConvertDataset returned an error", err)
				return err
			}
			newList = append(newList, n)
		}

		for i, n := range newList {
			if n == nil {
				log.Error(ctx, "seedDB endpoint: why are we trying to Upsert an empty new dataset?", nil, log.Data{"dataset_id": n, "index": i})
				continue
			}

			if err := api.dataStore.Backend.UpsertLDDataset(ctx, n.Identifier, n); err != nil {
				log.Error(ctx, "seedDB endpoint: failed to upsert dataset document", err, log.Data{"dataset_id": n.Identifier})
				return err
			}
		}

		instances, _, err := api.dataStore.Backend.GetInstances(ctx, nil, nil, 0, 500)
		if err != nil {
			log.Error(ctx, "seedDB endpoint: datastore.getDatasets returned an error", err)
			return err
		}

		log.Info(ctx, "ranging over instances list", log.Data{"length": len(instances)})
		for _, i := range instances {
			new, err := api.convertInstance(ctx, i)
			if err != nil {
				log.Error(ctx, "seedDB endpoint: convertInstance returned an error", err)
				return err
			}
			log.Info(ctx, "converted instance", log.Data{"id": new.InstanceID})

			if err := api.dataStore.Backend.UpsertLDInstance(ctx, new.InstanceID, new); err != nil {
				log.Error(ctx, "seedDB endpoint: failed to upsert instance document", err, log.Data{"instance_id": new.InstanceID})
				return err
			}

			if err := api.storeDimensions(ctx, i); err != nil {
				log.Error(ctx, "seedDB endpoint: failed to insert dimensions for instance", err, log.Data{"instance_id": new.InstanceID})
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
	log.Info(ctx, "seedDB endpoint: request successful")
}

func (api *DatasetAPI) convertDataset(ctx context.Context, old *models.Dataset, id string) (*models.LDDataset, error) {
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

func (api *DatasetAPI) convertInstance(ctx context.Context, old *models.Instance) (*models.LDInstance, error) {

	new := &models.LDInstance{
		Events:     old.Events,
		InstanceID: old.InstanceID,
		LDEdition: models.LDEdition{
			LastUpdated:  old.LastUpdated,
			CollectionID: old.CollectionID,
			State:        old.State,
			DCATDataset: models.DCATDataset{
				Edition: old.Edition,
				Version: old.Version,
			},
		},
	}

	var err error
	new.ReleaseDate, err = time.Parse(time.RFC3339, old.ReleaseDate)
	if err != nil {
		log.Error(ctx, "convertDatasets endpoint: cannot convert release date", err)
		return nil, err
	}

	if old.LatestChanges != nil {
		desc := []string{}
		for _, change := range *old.LatestChanges {
			desc = append(desc, change.Description)
		}
		new.LDEdition.DCATDataset.VersionNotes = desc
	}

	new.Links = &models.LDInstanceLinks{
		EditionLinks: models.EditionLinks{
			SelfLink: models.SelfLink{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("/instances/%s", new.InstanceID),
					ID:   new.InstanceID,
				},
			},
			DatasetLink: models.DatasetLink{
				Dataset: old.Links.Dataset,
			},
		},
	}

	if old.Links != nil {
		if old.Links.Job != nil {
			new.Links.Job = old.Links.Job
		}

		if old.Links.Version != nil {
			new.Links.Version = old.Links.Version
		}

		if old.Links.Edition != nil {
			new.Links.Edition = old.Links.Edition
		}
	}

	return new, nil
}

func (api *DatasetAPI) storeDimensions(ctx context.Context, old *models.Instance) error {
	if len(old.Dimensions) == 0 {
		return nil
	}

	links := &models.LDDimensionLinks{
		Instance: &models.LinkObject{
			ID: old.InstanceID,
		},
		DatasetLink: models.DatasetLink{
			Dataset: &models.LinkObject{
				ID: old.Links.Dataset.ID,
			},
		},
		EditionLink: models.EditionLink{
			Edition: &models.LinkObject{
				ID: old.Links.Edition.ID,
			},
		},
	}

	if old.Links.Version != nil {
		links.Version = &models.LinkObject{
			ID: old.Links.Version.ID,
		}
	}

	for _, d := range old.Dimensions {
		tmpLinks := links
		tmpLinks.Self = &models.LinkObject{
			ID: d.Name,
		}
		newD := &models.LDDimension{
			EmbeddedDimension: models.EmbeddedDimension{
				CodeList:   d.HRef,
				Identifier: d.ID,
				Name:       d.Name,
				Label:      d.Label,
			},
			Links: tmpLinks,
		}

		//insert to DB
		if err := api.dataStore.Backend.InsertLDDimension(ctx, newD); err != nil {
			return err
		}
	}

	return nil
}
