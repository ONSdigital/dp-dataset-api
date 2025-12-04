package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	"github.com/ONSdigital/dp-net/v3/links"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

// This function returns a list of editions, the total count of editions that match the query parameters and an error
// TODO: Refactor this to have named results
//
//nolint:gocognit,gocyclo,gocritic // Naming results requires some refactoring here.
func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}
	authorised := api.checkUserPermission(r, logData, datasetEditionVersionReadPermission)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	logData["state"] = state

	if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
		log.Error(ctx, "getEditions endpoint: unable to find dataset", err, logData)
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return nil, 0, err
	}

	datasetType, err := api.dataStore.Backend.GetDatasetType(ctx, datasetID, authorised)
	if err != nil {
		log.Error(ctx, "getEdition endpoint: unable to find dataset type", err, logData)
		return nil, 0, err
	}

	var results []*models.EditionUpdate
	var totalCount int

	if datasetType == models.Static.String() {
		var versionResults []*models.Version
		var unpublishedVersion *models.Version

		versionResults, totalCount, err = api.dataStore.Backend.GetAllStaticVersions(ctx, datasetID, state, offset, limit)
		if err != nil {
			log.Error(ctx, "getEditions endpoint: unable to find versions for dataset", err, logData)
			if err == errs.ErrVersionNotFound {
				http.Error(w, errs.ErrEditionsNotFound.Error(), http.StatusNotFound)
			} else {
				http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
			}
			return nil, 0, err
		}

		editionMap := make(map[string][]*models.Version)
		for _, version := range versionResults {
			editionMap[version.Edition] = append(editionMap[version.Edition], version)
		}

		for editionID := range editionMap {
			publishedVersion, err := api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, editionID, models.PublishedState)
			if err != nil && err != errs.ErrVersionNotFound {
				log.Error(ctx, "getEdition endpoint: unable to find latest published static version", err, logData)
				return nil, 0, err
			}

			if authorised {
				unpublishedVersion, err = api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, editionID, "")
				if err != nil && err != errs.ErrVersionNotFound {
					log.Error(ctx, "getEdition endpoint: unable to find latest unpublished static version", err, logData)
					return nil, 0, err
				}
			}

			edition, err := utils.MapVersionsToEditionUpdate(publishedVersion, unpublishedVersion)
			if err != nil {
				log.Error(ctx, "getEditions endpoint: failed to map versions to edition", err, logData)
				return nil, 0, err
			}
			results = append(results, edition)
		}
	} else {
		results, totalCount, err = api.dataStore.Backend.GetEditions(ctx, datasetID, state, offset, limit, authorised)
		if err != nil {
			log.Error(ctx, "getEditions endpoint: unable to find editions for dataset", err, logData)
			if err == errs.ErrEditionNotFound {
				http.Error(w, err.Error(), http.StatusNotFound)
			} else {
				http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
			}
			return nil, 0, err
		}
	}

	if api.enableURLRewriting {
		datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())

		if authorised {
			editionsResponse, err := utils.RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder, api.urlBuilder.GetDownloadServiceURL())
			if err != nil {
				log.Error(ctx, "getEditions endpoint: failed to rewrite editions with authorisation", err, logData)
				return nil, 0, err
			}
			log.Info(ctx, "getEditions endpoint: get all editions with auth", logData)
			return editionsResponse, totalCount, nil
		}

		editionsResponse, err := utils.RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder, api.urlBuilder.GetDownloadServiceURL())
		if err != nil {
			log.Error(ctx, "getEditions endpoint: failed to rewrite editions without authorisation", err, logData)
			return nil, 0, err
		}
		log.Info(ctx, "getEditions endpoint: get all editions without auth", logData)
		return editionsResponse, totalCount, nil
	}

	if authorised {
		log.Info(ctx, "getEditions endpoint: get all edition with auth", logData)
		return results, totalCount, nil
	}

	publicResults := make([]*models.Edition, 0, len(results))
	for i := range results {
		publicResults = append(publicResults, results[i].Current)
	}
	log.Info(ctx, "getEditions endpoint: get all edition without auth", logData)
	return publicResults, totalCount, nil
}

//nolint:gocognit,gocyclo // cognitive complexity 36 (> 30) is acceptable for now
func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	editionID := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": editionID}

	b, err := func() ([]byte, error) {
		authorised := api.checkUserPermission(r, logData, datasetEditionVersionReadPermission)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		datasetType, err := api.dataStore.Backend.GetDatasetType(ctx, datasetID, authorised)
		if err != nil {
			log.Error(ctx, "getEdition endpoint: unable to find dataset type", err, logData)
			return nil, err
		}

		var edition *models.EditionUpdate
		var unpublishedVersion *models.Version

		if datasetType == models.Static.String() {
			publishedVersion, err := api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, editionID, models.PublishedState)
			if err != nil && err != errs.ErrVersionNotFound {
				log.Error(ctx, "getEdition endpoint: unable to find latest published static version", err, logData)
				return nil, err
			}

			if authorised {
				unpublishedVersion, err = api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, editionID, "")
				if err != nil && err != errs.ErrVersionNotFound {
					log.Error(ctx, "getEdition endpoint: unable to find latest unpublished static version", err, logData)
					return nil, err
				}
			}

			edition, err = utils.MapVersionsToEditionUpdate(publishedVersion, unpublishedVersion)
			if err != nil {
				log.Error(ctx, "getEdition endpoint: failed to map version to edition", err, logData)
				return nil, err
			}
		} else {
			edition, err = api.dataStore.Backend.GetEdition(ctx, datasetID, editionID, state)
			if err != nil {
				log.Error(ctx, "getEdition endpoint: unable to find edition", err, logData)
				return nil, err
			}
		}

		var editionResponse interface{}

		if api.enableURLRewriting {
			datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())

			if authorised {
				editionResponse, err = utils.RewriteEditionWithAuth(ctx, edition, datasetLinksBuilder, api.urlBuilder.GetDownloadServiceURL())
				if err != nil {
					log.Error(ctx, "getEdition endpoint: failed to rewrite edition with authorisation", err, logData)
					return nil, err
				}
				log.Info(ctx, "getEdition endpoint: get edition with auth", logData)
			} else {
				editionResponse, err = utils.RewriteEditionWithoutAuth(ctx, edition, datasetLinksBuilder, api.urlBuilder.GetDownloadServiceURL())
				if err != nil {
					log.Error(ctx, "getEdition endpoint: failed to rewrite edition without authorisation", err, logData)
					return nil, err
				}
				log.Info(ctx, "getEdition endpoint: get edition without auth", logData)
			}
		} else {
			var b []byte
			if authorised {
				// User has valid authentication to get raw edition document
				b, err = json.Marshal(edition)
				if err != nil {
					log.Error(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", err, logData)
					return nil, err
				}
				log.Info(ctx, "getEdition endpoint: get edition with auth", logData)
			} else {
				// User is not authenticated and hence has only access to current sub document
				b, err = json.Marshal(edition.Current)
				if err != nil {
					log.Error(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", err, logData)
					return nil, err
				}
				log.Info(ctx, "getEdition endpoint: get edition without auth", logData)
			}
			return b, nil
		}

		b, err := json.Marshal(editionResponse)
		if err != nil {
			log.Error(ctx, "getEdition endpoint: failed to marshal edition resource into bytes", err, logData)
			return nil, err
		}
		log.Info(ctx, "getEdition endpoint: get edition", logData)
		return b, nil
	}()

	if err != nil {
		if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrVersionNotFound {
			http.Error(w, errs.ErrEditionNotFound.Error(), http.StatusNotFound)
		} else {
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(ctx, "getEdition endpoint: failed to write byte to response", err, logData)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}
	log.Info(ctx, "getEdition endpoint: request successful", logData)
}
