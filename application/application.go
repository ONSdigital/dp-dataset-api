package application

import (
	"context"
	"fmt"
	"strconv"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"

	"github.com/ONSdigital/dp-dataset-api/store"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/jinzhu/copier"
)

const (
	reqUser   = "req_user"
	reqCaller = "req_caller"
)

const (
	//nolint:gosec // This is not a hardcoded credential.
	downloadServiceToken = "X-Download-Service-Token"
	updateVersionAction  = "updateVersion"
	hasDownloads         = "has_downloads"
)

var (
	trueStringified = strconv.FormatBool(true)
)

// VersionDetails contains the details that uniquely identify a version resource
type VersionDetails struct {
	datasetID string
	edition   string
	version   string
}

type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

type StateMachineDatasetAPI struct {
	dataStore          store.DataStore
	downloadGenerators map[models.DatasetType]DownloadsGenerator
	stateMachine       *StateMachine
}

func Setup(ctx context.Context, router *mux.Router, dataStoreVal store.DataStore, downloadGenerators map[models.DatasetType]DownloadsGenerator, stateMachine *StateMachine) *StateMachineDatasetAPI {
	newDS := &StateMachineDatasetAPI{
		dataStore:          dataStoreVal,
		downloadGenerators: downloadGenerators,
		stateMachine:       stateMachine,
	}

	return newDS
}

func (v VersionDetails) baseLogData() log.Data {
	return log.Data{"dataset_id": v.datasetID, "edition": v.edition, "version": v.version}
}

func (smDS *StateMachineDatasetAPI) AmendVersion(vars map[string]string, version *models.Version, ctx context.Context) error {

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

	currentDataset, currentVersion, versionUpdate, err := smDS.PopulateVersionInfo(ctx, version, versionDetails)
	if err != nil {
		log.Error(ctx, "amendVersion: creating models failed", err)
		return err
	}

	if err := smDS.stateMachine.Transition(versionUpdate, versionUpdate.State, currentDataset.Next.State, smDS, ctx, currentDataset, currentVersion, versionUpdate, versionDetails); err != nil {
		log.Error(ctx, "amendVersion: state machine transition failed", err)
		return err
	}

	if vars[hasDownloads] != trueStringified {
		data["updated_state"] = versionUpdate.State

		if versionUpdate.State == models.AssociatedState && currentVersion.State != models.AssociatedState {

			if err := smDS.associateVersion(ctx, currentVersion, versionUpdate, versionDetails); err != nil {
				log.Error(ctx, "amendVersion: failed associating version", err)
				return err
			}
		}
	}

	return nil
}

func (smDS *StateMachineDatasetAPI) associateVersion(ctx context.Context, currentVersion, versionDoc *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()
	data["type"] = currentVersion.Type
	data["version_update"] = versionDoc
	log.Info(ctx, "associateVersion: associated version", data)

	associateVersionErr := func() error {
		if err := smDS.dataStore.Backend.UpdateDatasetWithAssociation(ctx, versionDetails.datasetID, versionDoc.State, versionDoc); err != nil {
			log.Error(ctx, "associateVersion: failed to update dataset document after a version of a dataset has been associated with a collection", err, data)
			return err
		}

		// Get the download generator from the map, depending of the Version document type
		t, err := models.GetDatasetType(currentVersion.Type)
		if err != nil {
			return fmt.Errorf("error getting type of version: %w", err)
		}
		generator, ok := smDS.downloadGenerators[t]
		if !ok {
			return fmt.Errorf("no downloader available for type %s", t.String())
		}

		if err := generator.Generate(ctx, versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
			data["instance_id"] = versionDoc.ID
			data["state"] = versionDoc.State
			log.Error(ctx, "associateVersion: error while attempting to generate full dataset version downloads on version association", err, data)
			return err
		}
		data["type"] = t.String()
		log.Info(ctx, "associateVersion: generated full dataset version downloads", data)
		return nil
	}()

	if associateVersionErr != nil {
		return associateVersionErr
	}

	log.Info(ctx, "associate version completed successfully", data)
	return associateVersionErr
}

func (smDS *StateMachineDatasetAPI) PopulateVersionInfo(ctx context.Context, versionUpdate *models.Version, versionDetails VersionDetails) (currentDataset *models.DatasetUpdate, currentVersion, combinedVersionUpdate *models.Version, err error) {
	data := versionDetails.baseLogData()

	reqID := ctx.Value(dprequest.RequestIdKey) // used to differentiate logs of concurrent calls to this function from different services

	versionNumber, err := models.ParseAndValidateVersionNumber(ctx, versionDetails.version)
	if err != nil {
		log.Error(ctx, "UpdateVersion: invalid version request", err, data)
		return nil, nil, nil, err
	}

	currentDataset, err = smDS.dataStore.Backend.GetDataset(ctx, versionDetails.datasetID)
	if err != nil {
		log.Error(ctx, "UpdateVersion: datastore.getDataset returned an error", err, data)
		return nil, nil, nil, err
	}

	if err = smDS.dataStore.Backend.CheckEditionExists(ctx, versionDetails.datasetID, versionDetails.edition, ""); err != nil {
		log.Error(ctx, "UpdateVersion: failed to find edition of dataset", err, data)
		return nil, nil, nil, err
	}

	currentVersion, err = smDS.dataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
	if err != nil {
		log.Error(ctx, "UpdateVersion: datastore.GetVersion returned an error", err, data)
		return nil, nil, nil, err
	}

	// doUpdate is an aux function that combines the existing version document with the update received in the body request,
	// then it validates the new model, and performs the update in MongoDB, passing the existing model ETag (if it exists) to be used in the query selector
	// Note that the combined version update does not mutate versionUpdate because multiple retries might generate a different value depending on the currentVersion at that point.
	var doUpdate = func() error {
		combinedVersionUpdate, err = populateNewVersionDoc(currentVersion, versionUpdate)
		if err != nil {
			return err
		}

		data["updated_version"] = combinedVersionUpdate

		if err = models.ValidateVersion(combinedVersionUpdate); err != nil {
			log.Error(ctx, "UpdateVersion: failed validation check for version update", err)
			return err
		}
		return nil
	}

	if err := doUpdate(); err != nil {
		if err == errs.ErrDatasetNotFound {
			log.Info(ctx, "get version info", data)
			currentVersion, err = smDS.dataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
			if err != nil {
				log.Error(ctx, "UpdateVersion: datastore.GetVersion returned an error", err, data)
				return nil, nil, nil, err
			}

			if err = doUpdate(); err != nil {
				log.Error(ctx, "UpdateVersion: failed to get version info", err, data)
				return nil, nil, nil, err
			}
		} else {
			return nil, nil, nil, err
		}
	}

	data["type"] = currentVersion.Type
	data["reqID"] = reqID
	log.Info(ctx, "update version completed successfully", data)

	return currentDataset, currentVersion, combinedVersionUpdate, nil
}

func populateNewVersionDoc(currentVersion, originalVersion *models.Version) (*models.Version, error) {
	var version models.Version
	err := copier.Copy(&version, originalVersion) // create local copy that escapes to the HEAP at the end of this function
	if err != nil {
		return nil, err
	}

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

func (smDS *StateMachineDatasetAPI) publishDataset(ctx context.Context, currentDataset *models.DatasetUpdate, version *models.Version) error {
	if version != nil {
		currentDataset.Next.CollectionID = ""
		currentDataset.Next.Links.LatestVersion = &models.LinkObject{
			ID:   version.Links.Version.ID,
			HRef: version.Links.Version.HRef,
		}
	}

	currentDataset.Next.State = models.PublishedState
	currentDataset.Next.LastUpdated = time.Now()

	// newDataset.Next will not be cleaned up due to keeping request to mongo
	// idempotent; for instance if an authorised user double clicked to update
	// dataset, the next sub document would not exist to create the correct
	// current sub document on the second click
	newDataset := &models.DatasetUpdate{
		ID:      currentDataset.ID,
		Current: currentDataset.Next,
		Next:    currentDataset.Next,
	}

	if err := smDS.dataStore.Backend.UpsertDataset(ctx, currentDataset.ID, newDataset); err != nil {
		log.Error(ctx, "unable to update dataset", err, log.Data{"dataset_id": currentDataset.ID})
		return err
	}

	return nil
}
