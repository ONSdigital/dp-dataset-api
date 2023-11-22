package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

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
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetV2Dataset(ctx, datasetID, true)
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
				HRef: fmt.Sprintf("%s/v2/datasets/%s", api.host, dataset.Identifier),
			},
			Editions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, dataset.Identifier),
			},
		}

		// replace embedded @id field with proper URLs and set latest version link
		if dataset.Embedded != nil && len(dataset.Embedded.Editions) > 0 {
			for i, ed := range dataset.Embedded.Editions {
				s := fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, dataset.Identifier, ed.ID)
				dataset.Embedded.Editions[i].ID = s
			}

			dataset.Links.LatestVersion = &models.LinkObject{
				HRef: dataset.Embedded.Editions[0].ID,
			}
		}

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

func (api *DatasetAPI) getV2Editions(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]

	b, err := func() ([]byte, error) {
		list, _, err := api.dataStore.Backend.GetV2Editions(ctx, datasetID, "published", 0, 100, true)
		if err != nil {
			log.Error(ctx, "getV2Editions endpoint: datastore.getV2Datasets returned an error", err)
			return nil, err
		}

		//linked data fields - @id per item
		for i, l := range list {
			list[i].ID = fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, datasetID, l.Edition)
		}

		// TODO: fix pagination
		page := &models.EditionList{
			Items: list,
			Page: models.Page{
				TotalCount: len(list),
			},
			LinkedData: models.LinkedData{
				Context: "cdn.ons.gov.uk/context.json",
			},
			Links: &models.PageLinks{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, datasetID),
				},
				Next: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, datasetID),
				},
				Prev: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, datasetID),
				},
			},
		}

		groups := []string{"editions"}
		b, err := marshal(page, groups...)
		if err != nil {
			log.Error(ctx, "getV2Editions endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Editions endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Editions endpoint: request successful")
}

func (api *DatasetAPI) getV2Edition(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	editionID := vars["edition"]

	b, err := func() ([]byte, error) {
		edition, err := api.dataStore.Backend.GetV2Edition(ctx, datasetID, editionID, "published", true)
		if err != nil {
			log.Error(ctx, "getV2Edition endpoint: datastore.getV2Edition returned an error", err)
			return nil, err
		}

		edition.LinkedData = models.LinkedData{
			Context: "cdn.ons.gov.uk/context.json",
			ID:      fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, datasetID, edition.Edition),
			Type:    []string{"dcat:dataset"},
		}

		edition.Links = &models.EditionLinks{
			DatasetLink: models.DatasetLink{
				Dataset: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s", api.host, datasetID),
				},
			},
			Editions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, datasetID),
			},
			Versions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions", api.host, datasetID, edition.Edition),
			},
			SelfLink: models.SelfLink{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, datasetID, edition.Edition),
				},
			},
		}

		// replace embedded @id field with proper URLs and set latest version link
		if edition.Embedded != nil && len(edition.Embedded.Versions) > 0 {
			for i, ed := range edition.Embedded.Versions {
				s := fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d", api.host, datasetID, edition.Edition, ed.Version)
				edition.Embedded.Versions[i].ID = s
			}

			edition.Links.LatestVersion = &models.LinkObject{
				HRef: edition.Embedded.Versions[0].ID,
			}
		}

		// TODO set etag header?

		groups := []string{"edition"}
		b, err := marshal(edition, groups...)
		if err != nil {
			log.Error(ctx, "getV2Edition endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Edition endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Edition endpoint: request successful")
}

func (api *DatasetAPI) getV2Versions(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	editionID := vars["edition"]

	b, err := func() ([]byte, error) {
		list, _, err := api.dataStore.Backend.GetV2Versions(ctx, datasetID, editionID, "published", 0, 100, true)
		if err != nil {
			log.Error(ctx, "getV2Versions endpoint: datastore.getV2Datasets returned an error", err)
			return nil, err
		}

		//linked data fields - @id per item
		for i, l := range list {
			list[i].ID = fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%s", api.host, datasetID, editionID, strconv.Itoa(l.Version))
		}

		// TODO: fix pagination
		page := &models.EditionList{
			Items: list,
			Page: models.Page{
				TotalCount: len(list),
			},
			LinkedData: models.LinkedData{
				Context: "cdn.ons.gov.uk/context.json",
			},
			Links: &models.PageLinks{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions", api.host, datasetID, editionID),
				},
				Next: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions", api.host, datasetID, editionID),
				},
				Prev: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions", api.host, datasetID, editionID),
				},
			},
		}

		groups := []string{"versions"}
		b, err := marshal(page, groups...)
		if err != nil {
			log.Error(ctx, "getV2Versions endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Versions endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Versions endpoint: request successful")
}

func (api *DatasetAPI) getV2Version(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	editionID := vars["edition"]
	versionNumber := vars["version"]

	b, err := func() ([]byte, error) {

		versionID, err := models.ParseAndValidateVersionNumber(ctx, versionNumber)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: invalid version", err)
			return nil, err
		}

		edition, err := api.dataStore.Backend.GetV2Version(ctx, datasetID, editionID, versionID, "published", true)
		if err != nil {
			log.Error(ctx, "getV2Version endpoint: datastore.getV2Version returned an error", err)
			return nil, err
		}

		edition.LinkedData = models.LinkedData{
			Context: "cdn.ons.gov.uk/context.json",
			ID:      fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d", api.host, datasetID, editionID, versionID),
			Type:    []string{"dcat:dataset"},
		}

		edition.Links = &models.EditionLinks{
			DatasetLink: models.DatasetLink{
				Dataset: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s", api.host, datasetID),
				},
			},
			Editions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions", api.host, datasetID),
			},
			EditionLink: models.EditionLink{
				Edition: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, datasetID, edition.Edition),
				},
			},
			Versions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions", api.host, datasetID, edition.Edition),
			},
			SelfLink: models.SelfLink{
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d", api.host, datasetID, edition.Edition, versionID),
				},
			},
			Dimensions: &models.LinkObject{
				HRef: fmt.Sprintf("%s/v2/datasets/%s/editions/%s/versions/%d/dimensions", api.host, datasetID, edition.Edition, versionID),
			},
		}

		// replace embedded @id field with proper URLs and set latest version link
		// if dataset.Embedded != nil && len(dataset.Embedded.Editions) > 0 {
		// 	for i, ed := range dataset.Embedded.Editions {
		// 		s := fmt.Sprintf("%s/v2/datasets/%s/editions/%s", api.host, dataset.Identifier, ed.ID)
		// 		dataset.Embedded.Editions[i].ID = s
		// 	}

		// 	dataset.Links.LatestVersion = &models.LinkObject{
		// 		HRef: dataset.Embedded.Editions[0].ID,
		// 	}
		// }

		// TODO set etag header?

		groups := []string{"version"}
		b, err := marshal(edition, groups...)
		if err != nil {
			log.Error(ctx, "getV2Version endpoint: marshal returned an error", err, log.Data{"groups": groups})
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
		log.Error(ctx, "getV2Version endpoint: error writing bytes to response", err)
		handleDatasetAPIErr(ctx, err, w, nil)
	}
	log.Info(ctx, "getV2Version endpoint: request successful")
}
