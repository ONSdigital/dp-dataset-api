package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dpresponse "github.com/ONSdigital/dp-net/v2/handlers/response"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/jinzhu/copier"
)

var (
	// errors that map to a HTTP 404 response
	notFound = map[error]bool{
		errs.ErrDatasetNotFound: true,
		errs.ErrEditionNotFound: true,
		errs.ErrVersionNotFound: true,
	}

	// errors that map to a HTTP 400 response
	badRequest = map[error]bool{
		errs.ErrUnableToParseJSON:                      true,
		models.ErrPublishedVersionCollectionIDInvalid:  true,
		models.ErrAssociatedVersionCollectionIDInvalid: true,
		models.ErrVersionStateInvalid:                  true,
	}

	// HTTP 500 responses with a specific message
	internalServerErrWithMessage = map[error]bool{
		errs.ErrResourceState: true,
	}
)

// getVersions returns a list of versions, the total count of versions that match the query parameters and an error
// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit,gocritic // high cyclomactic & cognitive complexity not in scope for maintenance. Named results requires similar levels of refactoring.
func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}
	var err error

	list, totalCount, err := func() ([]models.Version, int, error) {
		authorised := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
			log.Error(ctx, "failed to find dataset for list of versions", err, logData)
			return nil, 0, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(ctx, datasetID, edition, state); err != nil {
			log.Error(ctx, "failed to find edition for list of versions", err, logData)
			return nil, 0, err
		}

		results, totalCount, err := api.dataStore.Backend.GetVersions(ctx, datasetID, edition, state, offset, limit)
		if err != nil {
			log.Error(ctx, "failed to find any versions for dataset edition", err, logData)
			return nil, 0, err
		}

		var hasInvalidState bool
		for i := range results {
			item := &results[i]
			if err = models.CheckState("version", item.State); err != nil {
				hasInvalidState = true
				log.Error(ctx, "unpublished version has an invalid state", err, log.Data{"state": item.State})
			}

			// Only the download service should have access to the
			// public/private download fields
			if r.Header.Get(downloadServiceToken) != api.downloadServiceToken {
				if item.Downloads != nil {
					if item.Downloads.CSV != nil {
						item.Downloads.CSV.Private = ""
						item.Downloads.CSV.Public = ""
					}
					if item.Downloads.XLS != nil {
						item.Downloads.XLS.Private = ""
						item.Downloads.XLS.Public = ""
					}
					if item.Downloads.CSVW != nil {
						item.Downloads.CSVW.Private = ""
						item.Downloads.CSVW.Public = ""
					}
				}
			}
		}

		if hasInvalidState {
			return nil, 0, err
		}

		return results, totalCount, nil
	}()

	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	return list, totalCount, nil
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionNumber := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": versionNumber}

	v, getVersionErr := func() (*models.Version, error) {
		authorised := api.authenticate(r, logData)

		versionID, err := models.ParseAndValidateVersionNumber(ctx, versionNumber)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: invalid version", err, logData)
			return nil, err
		}

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
			log.Error(ctx, "failed to find dataset", err, logData)
			return nil, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(ctx, datasetID, edition, state); err != nil {
			log.Error(ctx, "failed to find edition for dataset", err, logData)
			return nil, err
		}

		version, err := api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionID, state)
		if err != nil {
			log.Error(ctx, "failed to find version for dataset edition", err, logData)
			return nil, err
		}

		version.Links.Self.HRef = version.Links.Version.HRef

		if err = models.CheckState("version", version.State); err != nil {
			log.Error(ctx, "unpublished version has an invalid state", err, log.Data{"state": version.State})
			return nil, errs.ErrResourceState
		}

		// Only the download service should not have access to the public/private download
		// fields
		if r.Header.Get(downloadServiceToken) != api.downloadServiceToken {
			if version.Downloads != nil {
				if version.Downloads.CSV != nil {
					version.Downloads.CSV.Private = ""
					version.Downloads.CSV.Public = ""
				}
				if version.Downloads.XLS != nil {
					version.Downloads.XLS.Private = ""
					version.Downloads.XLS.Public = ""
				}
				if version.Downloads.CSVW != nil {
					version.Downloads.CSVW.Private = ""
					version.Downloads.CSVW.Public = ""
				}
			}
		}
		return version, nil
	}()

	if getVersionErr != nil {
		handleVersionAPIErr(ctx, getVersionErr, w, logData)
		return
	}

	setJSONContentType(w)
	if v.ETag != "" {
		dpresponse.SetETag(w, v.ETag)
	}

	versionBytes, err := json.Marshal(v)
	if err != nil {
		log.Error(ctx, "failed to marshal version resource into bytes", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}

	_, err = w.Write(versionBytes)
	if err != nil {
		log.Error(ctx, "failed writing bytes to response", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.Info(ctx, "getVersion endpoint: request successful", logData)
}

func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	data := log.Data{
		"datasetID": vars["dataset_id"],
		"edition":   vars["edition"],
		"version":   vars["version"],
	}

	version, err := models.CreateVersion(r.Body, vars["dataset_id"])
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	err = api.smDatasetAPI.AmendVersion(r.Context(), vars, version)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putVersion endpoint: request successful", data)
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) detachVersion(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	log.Info(ctx, "detachVersion endpoint: endpoint called")

	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	if err := func() error {
		authorised := api.authenticate(r, logData)
		if !authorised {
			log.Error(ctx, "detachVersion endpoint: User is not authorised to detach a dataset version", errs.ErrUnauthorised, logData)
			return errs.ErrNotFound
		}

		versionID, err := models.ParseAndValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: invalid version request", err, logData)
			return err
		}

		editionDoc, err := api.dataStore.Backend.GetEdition(ctx, datasetID, edition, "")
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: Cannot find the specified edition", errs.ErrEditionNotFound, logData)
			return err
		}

		// Only permit detachment of the latest version.
		if editionDoc.Next.Links.LatestVersion.ID != version {
			log.Error(ctx, "detachVersion endpoint: Detach called againt a version other than latest, aborting", errs.ErrVersionAlreadyExists, logData)
			return errs.ErrVersionAlreadyExists
		}

		// Only permit detachment where state is edition-confirmed or associated
		state := editionDoc.Next.State
		if state != models.AssociatedState && state != models.EditionConfirmedState {
			log.Error(ctx, "detachVersion endpoint: You can only detach a version with a state of edition-confirmed or associated", errs.ErrIncorrectStateToDetach, logData)
			return errs.ErrIncorrectStateToDetach
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionID, editionDoc.Next.State)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: Cannot find the specified version", errs.ErrVersionNotFound, logData)
			return errs.ErrVersionNotFound
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: datastore.GetDatasets returned an error", err, logData)
			return err
		}

		// Detach the version
		update := &models.Version{
			State: models.DetachedState,
		}
		logData["updated_state"] = update.State
		if _, err = api.dataStore.Backend.UpdateVersion(ctx, versionDoc, update, headers.IfMatchAnyETag); err != nil {
			log.Error(ctx, "detachVersion endpoint: failed to update version document", err, logData)
			return err
		}

		// Only rollback dataset & edition if there's a "Current" sub-document to roll back to (i.e if a version has been published).
		if datasetDoc.Current != nil {
			// Rollback the edition
			editionDoc.Next = editionDoc.Current
			if err = api.dataStore.Backend.UpsertEdition(ctx, datasetID, edition, editionDoc); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update edition document", err, logData)
				return err
			}

			// Rollback the dataset
			datasetDoc.Next = datasetDoc.Current
			if err = api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update dataset document", err, logData)
				return err
			}
		} else {
			// For first (unpublished) versions:
			// delete edition doc
			if err := api.dataStore.Backend.DeleteEdition(ctx, editionDoc.ID); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to delete edition document", err, logData)
				return err
			}

			// remove edition and version links from datasetDoc
			if err := api.dataStore.Backend.RemoveDatasetVersionAndEditionLinks(ctx, datasetID); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update dataset document", err, logData)
				return err
			}
		}

		return nil
	}(); err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "detachVersion endpoint: request successful", logData)
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func populateNewVersionDoc(currentVersion, originalVersion *models.Version) (*models.Version, error) {
	var version models.Version
	err := copier.Copy(&version, originalVersion) // create local copy that escapes to the HEAP at the end of this function
	if err != nil {
		return nil, err
	}

	var alerts []models.Alert

	if version.Alerts != nil {
		alerts = append(alerts, *version.Alerts...)
		for i := range alerts {
			alerts[i].Date = currentVersion.ReleaseDate
		}
	}

	if alerts != nil {
		version.Alerts = &alerts
	}

	if version.CollectionID == "" {
		// will be checked later if state:published
		version.CollectionID = currentVersion.CollectionID
	}

	var latestChanges []models.LatestChange
	if currentVersion.LatestChanges != nil {
		latestChanges = append(latestChanges, *currentVersion.LatestChanges...)
	}

	if version.LatestChanges != nil {
		latestChanges = append(latestChanges, *version.LatestChanges...)
	}

	if latestChanges != nil {
		version.LatestChanges = &latestChanges
	}

	if version.ReleaseDate == "" {
		version.ReleaseDate = currentVersion.ReleaseDate
	}

	if version.State == "" {
		version.State = currentVersion.State
	}

	// when changing to (or updating) published state, ensure no CollectionID
	if version.State == models.PublishedState && version.CollectionID != "" {
		version.CollectionID = ""
	}

	if version.Temporal == nil {
		version.Temporal = currentVersion.Temporal
	}

	var spatial string

	// Get spatial link before overwriting the version links object below
	if version.Links != nil {
		if version.Links.Spatial != nil {
			if version.Links.Spatial.HRef != "" {
				spatial = version.Links.Spatial.HRef
			}
		}
	}

	version.ID = currentVersion.ID
	version.Links = nil
	if currentVersion.Links != nil {
		version.Links = currentVersion.Links.DeepCopy()
	}

	if spatial != "" {
		// In reality the current version will always have a link object, so
		// if/else statement should always fall into else block
		if version.Links == nil {
			version.Links = &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: spatial,
				},
			}
		} else {
			version.Links.Spatial = &models.LinkObject{
				HRef: spatial,
			}
		}
	}

	// TODO - Data Integrity - Updating downloads should be locked down to services
	// with permissions to do so, currently a user could update these fields

	log.Info(context.Background(), "DEBUG", log.Data{"downloads": version.Downloads, "currentDownloads": currentVersion.Downloads})
	if version.Downloads == nil {
		version.Downloads = currentVersion.Downloads
	} else {
		if version.Downloads.XLS == nil && currentVersion.Downloads != nil {
			version.Downloads.XLS = currentVersion.Downloads.XLS
		}

		if version.Downloads.XLSX == nil && currentVersion.Downloads != nil {
			version.Downloads.XLSX = currentVersion.Downloads.XLSX
		}

		if version.Downloads.CSV == nil && currentVersion.Downloads != nil {
			version.Downloads.CSV = currentVersion.Downloads.CSV
		}

		if version.Downloads.CSVW == nil && currentVersion.Downloads != nil {
			version.Downloads.CSVW = currentVersion.Downloads.CSVW
		}

		if version.Downloads.TXT == nil && currentVersion.Downloads != nil {
			version.Downloads.TXT = currentVersion.Downloads.TXT
		}
	}

	if version.UsageNotes == nil {
		version.UsageNotes = currentVersion.UsageNotes
	}

	return &version, nil
}

func handleVersionAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	var status int
	switch {
	case notFound[err]:
		status = http.StatusNotFound
	case badRequest[err]:
		status = http.StatusBadRequest
	case internalServerErrWithMessage[err]:
		status = http.StatusInternalServerError
	case strings.HasPrefix(err.Error(), "missing mandatory fields:"):
		status = http.StatusBadRequest
	case strings.HasPrefix(err.Error(), "invalid fields:"):
		status = http.StatusBadRequest
	case strings.HasPrefix(err.Error(), "invalid version requested"):
		status = http.StatusBadRequest
	default:
		err = fmt.Errorf("%s: %w", errs.ErrInternalServer.Error(), err)
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}
