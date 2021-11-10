package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	reqUser   = "req_user"
	reqCaller = "req_caller"
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

// VersionDetails contains the details that uniquely identify a version resource
type VersionDetails struct {
	datasetID string
	edition   string
	version   string
}

func (v VersionDetails) baseLogData() log.Data {
	return log.Data{"dataset_id": v.datasetID, "edition": v.edition, "version": v.version}
}

//getVersions returns a list of versions, the total count of versions that match the query parameters and an error
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

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.Error(ctx, "failed to find dataset for list of versions", err, logData)
			return nil, 0, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			log.Error(ctx, "failed to find edition for list of versions", err, logData)
			return nil, 0, err
		}

		results, totalCount, err := api.dataStore.Backend.GetVersions(ctx, datasetID, edition, state, offset, limit)
		if err != nil {
			log.Error(ctx, "failed to find any versions for dataset edition", err, logData)
			return nil, 0, err
		}

		var hasInvalidState bool
		for _, item := range results {
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

func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	b, getVersionErr := func() ([]byte, error) {
		authorised := api.authenticate(r, logData)

		versionId, err := models.ValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: invalid version", err, logData)
			return nil, err
		}

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.Error(ctx, "failed to find dataset", err, logData)
			return nil, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			log.Error(ctx, "failed to find edition for dataset", err, logData)
			return nil, err
		}

		results, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionId, state)
		if err != nil {
			log.Error(ctx, "failed to find version for dataset edition", err, logData)
			return nil, err
		}

		results.Links.Self.HRef = results.Links.Version.HRef

		if err = models.CheckState("version", results.State); err != nil {
			log.Error(ctx, "unpublished version has an invalid state", err, log.Data{"state": results.State})
			return nil, errs.ErrResourceState
		}

		// Only the download service should not have access to the public/private download
		// fields
		if r.Header.Get(downloadServiceToken) != api.downloadServiceToken {
			if results.Downloads != nil {
				if results.Downloads.CSV != nil {
					results.Downloads.CSV.Private = ""
					results.Downloads.CSV.Public = ""
				}
				if results.Downloads.XLS != nil {
					results.Downloads.XLS.Private = ""
					results.Downloads.XLS.Public = ""
				}
				if results.Downloads.CSVW != nil {
					results.Downloads.CSVW.Private = ""
					results.Downloads.CSVW.Public = ""
				}
			}
		}

		b, err := json.Marshal(results)
		if err != nil {
			log.Error(ctx, "failed to marshal version resource into bytes", err, logData)
			return nil, err
		}
		return b, nil
	}()

	if getVersionErr != nil {
		handleVersionAPIErr(ctx, getVersionErr, w, logData)
		return
	}

	setJSONContentType(w)
	_, err := w.Write(b)
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
	versionDetails := VersionDetails{
		datasetID: vars["dataset_id"],
		edition:   vars["edition"],
		version:   vars["version"],
	}
	data := log.Data{
		"datasetID": vars["dataset_id"],
		"edition":   vars["edition"],
		"version":   vars["version"],
	}

	currentDataset, currentVersion, versionUpdate, err := api.updateVersion(ctx, r.Body, versionDetails)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	// If update was to add downloads do not try to publish/associate version
	if vars[hasDownloads] != trueStringified {
		if versionUpdate.State == models.PublishedState {
			if err := api.publishVersion(ctx, currentDataset, currentVersion, versionUpdate, versionDetails); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return
			}
		}

		if versionUpdate.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
			if err := api.associateVersion(ctx, currentVersion, versionUpdate, versionDetails); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return
			}
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putVersion endpoint: request successful", data)
}

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

		versionId, err := models.ValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: invalid version request", err, logData)
			return err
		}

		editionDoc, err := api.dataStore.Backend.GetEdition(datasetID, edition, "")
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

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionId, editionDoc.Next.State)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: Cannot find the specified version", errs.ErrVersionNotFound, logData)
			return errs.ErrVersionNotFound
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: datastore.GetDatasets returned an error", err, logData)
			return err
		}

		// Detach the version
		versionDoc.State = models.DetachedState
		if err = api.dataStore.Backend.UpdateVersion(versionDoc.ID, versionDoc); err != nil {
			log.Error(ctx, "detachVersion endpoint: failed to update version document", err, logData)
			return err
		}

		// Only rollback dataset & edition if there's a "Current" sub-document to roll back to (i.e if a version has been published).
		if datasetDoc.Current != nil {
			// Rollback the edition
			editionDoc.Next = editionDoc.Current
			if err = api.dataStore.Backend.UpsertEdition(datasetID, edition, editionDoc); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update edition document", err, logData)
				return err
			}

			// Rollback the dataset
			datasetDoc.Next = datasetDoc.Current
			if err = api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update dataset document", err, logData)
				return err
			}
		} else {
			// For first (unpublished) versions:
			// delete edition doc
			if err := api.dataStore.Backend.DeleteEdition(editionDoc.ID); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to delete edition document", err, logData)
				return err
			}

			// remove edition and version links from datasetDoc
			if err := api.dataStore.Backend.RemoveDatasetVersionAndEditionLinks(datasetID); err != nil {
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

func (api *DatasetAPI) updateVersion(ctx context.Context, body io.ReadCloser, versionDetails VersionDetails) (*models.DatasetUpdate, *models.Version, *models.Version, error) {
	data := versionDetails.baseLogData()

	// attempt to update the version
	currentDataset, currentVersion, versionUpdate, err := func() (*models.DatasetUpdate, *models.Version, *models.Version, error) {

		version, err := models.ValidateVersionNumber(ctx, versionDetails.version)
		if err != nil {
			log.Error(ctx, "putVersion endpoint: invalid version request", err, data)
			return nil, nil, nil, err
		}

		versionUpdate, err := models.CreateVersion(body, versionDetails.datasetID)
		if err != nil {
			log.Error(ctx, "putVersion endpoint: failed to model version resource based on request", err, data)
			return nil, nil, nil, errs.ErrUnableToParseJSON
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(versionDetails.datasetID)
		if err != nil {
			log.Error(ctx, "putVersion endpoint: datastore.getDataset returned an error", err, data)
			return nil, nil, nil, err
		}

		if err = api.dataStore.Backend.CheckEditionExists(versionDetails.datasetID, versionDetails.edition, ""); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to find edition of dataset", err, data)
			return nil, nil, nil, err
		}

		currentVersion, err := api.dataStore.Backend.GetVersion(versionDetails.datasetID, versionDetails.edition, version, "")
		if err != nil {
			log.Error(ctx, "putVersion endpoint: datastore.GetVersion returned an error", err, data)
			return nil, nil, nil, err
		}

		// Combine update version document to existing version document
		populateNewVersionDoc(currentVersion, versionUpdate)
		data["updated_version"] = versionUpdate
		log.Info(ctx, "putVersion endpoint: combined current version document with update request", data)

		if err = models.ValidateVersion(versionUpdate); err != nil {
			log.Error(ctx, "putVersion endpoint: failed validation check for version update", err)
			return nil, nil, nil, err
		}

		if err := api.dataStore.Backend.UpdateVersion(versionUpdate.ID, versionUpdate); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update version document", err, data)
			return nil, nil, nil, err
		}
		return currentDataset, currentVersion, versionUpdate, nil
	}()

	// audit update unsuccessful if error
	if err != nil {
		return nil, nil, nil, err
	}

	log.Info(ctx, "update version completed successfully", data)
	return currentDataset, currentVersion, versionUpdate, nil
}

func (api *DatasetAPI) publishVersion(ctx context.Context, currentDataset *models.DatasetUpdate, currentVersion *models.Version, versionUpdate *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()
	log.Info(ctx, "attempting to publish version", data)
	err := func() error {
		editionDoc, err := api.dataStore.Backend.GetEdition(versionDetails.datasetID, versionDetails.edition, "")
		if err != nil {
			log.Error(ctx, "putVersion endpoint: failed to find the edition we're trying to update", err, data)
			return err
		}

		editionDoc.Next.State = models.PublishedState
		if err := editionDoc.PublishLinks(ctx, versionUpdate.Links.Version); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update the edition links for the version we're trying to publish", err, data)
			return err
		}

		editionDoc.Current = editionDoc.Next

		if err := api.dataStore.Backend.UpsertEdition(versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update edition during publishing", err, data)
			return err
		}

		log.Info(ctx, "DATASTORE", log.Data{
			"datastore": api.dataStore,
		})

		if err := api.dataStore.Backend.SetInstanceIsPublished(ctx, versionUpdate.ID); err != nil {
			if user := dprequest.User(ctx); user != "" {
				data[reqUser] = user
			}
			if caller := dprequest.Caller(ctx); caller != "" {
				data[reqCaller] = caller
			}
			err := errors.WithMessage(err, "putVersion endpoint: failed to set instance node is_published")
			log.Error(ctx, "failed to publish instance version", err, data)
			return err
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.publishDataset(ctx, currentDataset, versionUpdate); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update dataset document once version state changes to publish", err, data)
			return err
		}

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			// Lookup the download generator using the version document type
			t, err := models.GetDatasetType(currentVersion.Type)
			if err != nil {
				return fmt.Errorf("error getting type of version: %w", err)
			}
			generator, ok := api.downloadGenerators[t]
			if !ok {
				return fmt.Errorf("no downloader available for type %s", t.String())
			}
			// Send Kafka message.  The generator which is used depends on the type defined in VersionDoc.
			if err := generator.Generate(ctx, versionDetails.datasetID, versionUpdate.ID, versionDetails.edition, versionDetails.version); err != nil{
				data["instance_id"] = versionUpdate.ID
				data["state"] = versionUpdate.State
				log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version publish", err, data)
				return err
				// TODO - TECH DEBT - need to add an error event for this.  Kafka message perhaps.
			}
			log.Info(ctx, "putVersion endpoint: generated full dataset version downloads for type %s", data)
		}

		return nil
	}()

	if err != nil {
		return err
	}

	log.Info(ctx, "publish version completed successfully", data)
	return nil
}

func (api *DatasetAPI) associateVersion(ctx context.Context, currentVersion, versionDoc *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()
	data["type"] = currentVersion.Type

	associateVersionErr := func() error {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(versionDetails.datasetID, versionDoc.State, versionDoc); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update dataset document after a version of a dataset has been associated with a collection", err, data)
			return err
		}

		// Get the download generator from the map, depending of the Version document type
		t, err := models.GetDatasetType(currentVersion.Type)
		if err != nil {
			return fmt.Errorf("error getting type of version: %w", err)
		}
		generator, ok := api.downloadGenerators[t]
		// ToDo outsidecoder - Test by passing in the wrong mock, eg Filterable instead of Cantabular Table.  Create API with CMD mock and pass in Canti
		if !ok {
			return fmt.Errorf("no downloader available for type %s", t.String())
		}
		// ToDo outsidecoder - Pass the right mock and get Generate to fail by returning an error.New("Error message")
		if err := generator.Generate(ctx, versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil{
			data["instance_id"] = versionDoc.ID
			data["state"] = versionDoc.State
			log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association", err, data)
			return err
		}
		log.Info(ctx, "putVersion endpoint: generated full dataset version downloads for type %s", data)
		return nil
	}()

	if associateVersionErr != nil {
		return associateVersionErr
	}

	log.Info(ctx, "associate version completed successfully", data)
	return associateVersionErr
}

func populateNewVersionDoc(currentVersion *models.Version, version *models.Version) *models.Version {

	var alerts []models.Alert

	if version.Alerts != nil {
		alerts = append(alerts, *version.Alerts...)
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
	version.Links = currentVersion.Links

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
	if version.Downloads == nil {
		version.Downloads = currentVersion.Downloads
	} else {
		if version.Downloads.XLS == nil {
			if currentVersion.Downloads != nil && currentVersion.Downloads.XLS != nil {
				version.Downloads.XLS = currentVersion.Downloads.XLS
			}
		}

		if version.Downloads.CSV == nil {
			if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil {
				version.Downloads.CSV = currentVersion.Downloads.CSV
			}
		}

		if version.Downloads.CSVW == nil {
			if currentVersion.Downloads != nil && currentVersion.Downloads.CSVW != nil {
				version.Downloads.CSVW = currentVersion.Downloads.CSVW
			}
		}
	}

	if version.UsageNotes == nil {
		version.UsageNotes = currentVersion.UsageNotes
	}

	return version
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
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}
