package application

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/headers"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	filesAPISDK "github.com/ONSdigital/dp-files-api/sdk"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
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
	DataStore          store.DataStore
	DownloadGenerators map[models.DatasetType]DownloadsGenerator
	StateMachine       *StateMachine
}

func Setup(dataStoreVal store.DataStore, downloadGenerators map[models.DatasetType]DownloadsGenerator, stateMachine *StateMachine) *StateMachineDatasetAPI {
	newDS := &StateMachineDatasetAPI{
		DataStore:          dataStoreVal,
		DownloadGenerators: downloadGenerators,
		StateMachine:       stateMachine,
	}

	return newDS
}

func (v VersionDetails) baseLogData() log.Data {
	return log.Data{"dataset_id": v.datasetID, "edition": v.edition, "version": v.version}
}

func (smDS *StateMachineDatasetAPI) AmendVersion(ctx context.Context, vars map[string]string, version *models.Version) (*models.Version, error) {
	versionDetails := VersionDetails{
		datasetID: vars["dataset_id"],
		edition:   vars["edition"],
		version:   vars["version"],
	}

	if version.Type == models.Static.String() {
		lockID, lockErr := smDS.DataStore.Backend.AcquireVersionsLock(ctx, version.ID)
		if lockErr != nil {
			return nil, lockErr
		}
		defer func() {
			smDS.DataStore.Backend.UnlockVersions(ctx, lockID)
		}()
	} else {
		lockID, lockErr := smDS.DataStore.Backend.AcquireInstanceLock(ctx, version.ID)
		if lockErr != nil {
			return nil, lockErr
		}
		defer func() {
			smDS.DataStore.Backend.UnlockInstance(ctx, lockID)
		}()
	}

	currentVersion, versionUpdate, err := smDS.PopulateVersionInfo(ctx, version, versionDetails)
	if err != nil {
		log.Error(ctx, "amendVersion: creating models failed", err)
		return nil, err
	}

	if err := smDS.StateMachine.Transition(ctx, smDS, currentVersion, versionUpdate, versionDetails, vars[hasDownloads]); err != nil {
		log.Error(ctx, "amendVersion: state machine transition failed", err)
		return nil, err
	}

	return versionUpdate, nil
}

func (smDS *StateMachineDatasetAPI) PopulateVersionInfo(ctx context.Context, versionUpdate *models.Version, versionDetails VersionDetails) (currentVersion, combinedVersionUpdate *models.Version, err error) {
	data := versionDetails.baseLogData()

	reqID := ctx.Value(dprequest.RequestIdKey) // used to differentiate logs of concurrent calls to this function from different services

	versionNumber, err := models.ParseAndValidateVersionNumber(ctx, versionDetails.version)
	if err != nil {
		log.Error(ctx, "UpdateVersion: invalid version request", err, data)
		return nil, nil, err
	}

	if versionUpdate != nil {
		if versionUpdate.Type == models.Static.String() {
			if err = smDS.DataStore.Backend.CheckEditionExistsStatic(ctx, versionDetails.datasetID, versionDetails.edition, ""); err != nil {
				log.Error(ctx, "UpdateVersion: failed to find version of dataset", err, data)
				return nil, nil, err
			}
		} else {
			if err = smDS.DataStore.Backend.CheckEditionExists(ctx, versionDetails.datasetID, versionDetails.edition, ""); err != nil {
				log.Error(ctx, "UpdateVersion: failed to find edition of dataset", err, data)
				return nil, nil, err
			}
		}
	}

	if versionUpdate != nil {
		if versionUpdate.Type == models.Static.String() {
			currentVersion, err = smDS.DataStore.Backend.GetVersionStatic(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
			if err != nil {
				log.Error(ctx, "UpdateVersion: datastore.GetVersionStatic returned an error", err, data)
				return nil, nil, err
			}
		} else {
			currentVersion, err = smDS.DataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
			if err != nil {
				log.Error(ctx, "UpdateVersion: datastore.GetVersion returned an error", err, data)
				return nil, nil, err
			}
		}
	}

	if versionUpdate != nil && versionUpdate.Edition != "" && versionUpdate.Edition != currentVersion.Edition {
		if currentVersion.Type == models.Static.String() {
			err = smDS.DataStore.Backend.CheckEditionExistsStatic(ctx, versionDetails.datasetID, versionUpdate.Edition, "")
			if err == nil {
				log.Error(ctx, "UpdateVersion: edition-id already exists", errs.ErrEditionAlreadyExists, log.Data{
					"dataset_id":       versionDetails.datasetID,
					"existing_edition": currentVersion.Edition,
					"new_edition":      versionUpdate.Edition,
				})
				return nil, nil, errs.ErrEditionAlreadyExists
			} else if err != errs.ErrEditionNotFound {
				log.Error(ctx, "UpdateVersion: error checking if edition exists", err, data)
				return nil, nil, err
			}
		} else {
			log.Error(ctx, "UpdateVersion: attempted to update edition-id for non-static dataset type", errs.ErrInvalidDatasetTypeForEditionUpdate, data)
			return nil, nil, errs.ErrInvalidDatasetTypeForEditionUpdate
		}
	}

	// doUpdate is an aux function that combines the existing version document with the update received in the body request,
	// then it validates the new model, and performs the update in MongoDB, passing the existing model ETag (if it exists) to be used in the query selector
	// Note that the combined version update does not mutate versionUpdate because multiple retries might generate a different value depending on the currentVersion at that point.
	combinedVersionUpdate, err = populateNewVersionDoc(currentVersion, versionUpdate)
	if err != nil {
		return nil, nil, err
	}

	data["updated_version"] = combinedVersionUpdate

	if err = models.ValidateVersion(combinedVersionUpdate); err != nil {
		log.Error(ctx, "UpdateVersion: failed validation check for version update", err)
		return nil, nil, err
	}

	data["type"] = currentVersion.Type
	data["reqID"] = reqID
	log.Info(ctx, "update version completed successfully", data)

	return currentVersion, combinedVersionUpdate, nil
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

	version.ID = currentVersion.ID

	if version.Edition == "" {
		version.Edition = currentVersion.Edition
	}

	if version.Edition != "" && version.Edition != currentVersion.Edition {
		version.Links = updateEditionLinks(currentVersion, version.Edition)
	} else {
		version.Links = populateVersionLinks(version.Links, currentVersion.Links)
	}

	log.Info(context.Background(), "DEBUG", log.Data{"downloads": version.Downloads, "currentDownloads": currentVersion.Downloads})
	version.Downloads = populateDownloads(version.Downloads, currentVersion.Downloads)

	if version.Type == models.Static.String() {
		if version.Distributions == nil {
			version.Distributions = currentVersion.Distributions
		}
	} else {
		version.Distributions = nil
	}

	if version.UsageNotes == nil {
		version.UsageNotes = currentVersion.UsageNotes
	}

	return &version, nil
}

func updateEditionLinks(currentVersion *models.Version, newEdition string) *models.VersionLinks {
	if currentVersion.Links == nil {
		return nil
	}

	links := currentVersion.Links.DeepCopy()

	if links.Dataset == nil || links.Dataset.HRef == "" {
		return links
	}

	datasetHref := links.Dataset.HRef
	datasetID := links.Dataset.ID
	versionNum := currentVersion.Version

	host := strings.TrimSuffix(datasetHref, fmt.Sprintf("/datasets/%s", datasetID))

	if links.Edition != nil {
		links.Edition.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s", host, datasetID, newEdition)
		links.Edition.ID = newEdition
	}

	if links.Version != nil {
		links.Version.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%d", host, datasetID, newEdition, versionNum)
	}

	if links.Self != nil {
		links.Self.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%d", host, datasetID, newEdition, versionNum)
	}

	return links
}

func populateVersionLinks(versionLinks, currentVersionLinks *models.VersionLinks) *models.VersionLinks {
	var spatial string

	// Get spatial link before overwriting the version links object below
	if versionLinks != nil {
		if versionLinks.Spatial != nil {
			if versionLinks.Spatial.HRef != "" {
				spatial = versionLinks.Spatial.HRef
			}
		}
	}

	versionLinks = nil
	if currentVersionLinks != nil {
		versionLinks = currentVersionLinks.DeepCopy()
	}

	if spatial != "" {
		// In reality the current version will always have a link object, so
		// if/else statement should always fall into else block
		if versionLinks == nil {
			versionLinks = &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: spatial,
				},
			}
		} else {
			versionLinks.Spatial = &models.LinkObject{
				HRef: spatial,
			}
		}
	}
	return versionLinks
}

func populateDownloads(versionDownloads, currentVersionDownloads *models.DownloadList) *models.DownloadList {
	if versionDownloads == nil {
		versionDownloads = currentVersionDownloads
	} else {
		if versionDownloads.XLS == nil && currentVersionDownloads != nil {
			versionDownloads.XLS = currentVersionDownloads.XLS
		}

		if versionDownloads.XLSX == nil && currentVersionDownloads != nil {
			versionDownloads.XLSX = currentVersionDownloads.XLSX
		}

		if versionDownloads.CSV == nil && currentVersionDownloads != nil {
			versionDownloads.CSV = currentVersionDownloads.CSV
		}

		if versionDownloads.CSVW == nil && currentVersionDownloads != nil {
			versionDownloads.CSVW = currentVersionDownloads.CSVW
		}

		if versionDownloads.TXT == nil && currentVersionDownloads != nil {
			versionDownloads.TXT = currentVersionDownloads.TXT
		}
	}
	return versionDownloads
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

	if err := smDS.DataStore.Backend.UpsertDataset(ctx, currentDataset.ID, newDataset); err != nil {
		log.Error(ctx, "unable to update dataset", err, log.Data{"dataset_id": currentDataset.ID})
		return err
	}

	return nil
}

func AssociateVersion(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails,
	hasDownloads string) error {
	data := versionDetails.baseLogData()
	log.Info(ctx, "putVersion endpoint (associateVersion): beginning associate version", data)

	errModel := models.ValidateVersion(versionUpdate)
	if errModel != nil {
		log.Error(ctx, "State machine - Associating: ValidateVersion : failed to validate version", errModel, data)
		return errModel
	}

	_, err := UpdateVersionInfo(ctx, smDS, currentVersion, versionUpdate, versionDetails)
	if err != nil {
		log.Error(ctx, "State machine - Associating: UpdateVersionInfo : failed to update the version", err, data)
		return err
	}

	if currentVersion.Type != models.Static.String() {
		if hasDownloads != trueStringified {
			if versionUpdate.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
				if errVersion := smDS.DataStore.Backend.UpdateDatasetWithAssociation(ctx, versionUpdate.DatasetID, versionUpdate.State, versionUpdate); errVersion != nil {
					return errVersion
				}

				// Get the download generator from the map, depending of the Version document type
				t, err := models.GetDatasetType(currentVersion.Type)
				if err != nil {
					return fmt.Errorf("error getting type of version: %w", err)
				}
				generator, ok := smDS.DownloadGenerators[t]
				if !ok {
					return fmt.Errorf("no downloader available for type %s", t.String())
				}

				if err := generator.Generate(ctx, versionDetails.datasetID, versionUpdate.ID, versionDetails.edition, versionDetails.version); err != nil {
					data["instance_id"] = versionUpdate.ID
					data["state"] = versionUpdate.State
					log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association", err, data)
					return err
				}
				data["type"] = t.String()
				log.Info(ctx, "putVersion endpoint (associateVersion): generated full dataset version downloads", data)
			}
		}
	}

	return nil
}

//nolint:revive // hasDownloads is intentionally unused to be compatible with the State struct
func ApproveVersion(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails,
	hasDownloads string) error {
	data := versionDetails.baseLogData()
	log.Info(ctx, "putVersion endpoint (associateVersion): beginning associate version", data)

	errModel := models.ValidateVersion(versionUpdate)
	if errModel != nil {
		log.Error(ctx, "State machine - Approving: ValidateVersion : failed to validate version", errModel, data)
		return errModel
	}

	_, err := UpdateVersionInfo(ctx, smDS, currentVersion, versionUpdate, versionDetails)
	if err != nil {
		log.Error(ctx, "State machine - Approving: UpdateVersionInfo : failed to update the version", err, data)
		return err
	}

	return nil
}

func EditionConfirmVersion(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails,
	_ string) error {
	data := versionDetails.baseLogData()

	log.Info(ctx, "putVersion endpoint (editionConfirmVersion): beginning transition to edition-confirmed", data)

	errModel := models.ValidateVersion(versionUpdate)
	if errModel != nil {
		log.Error(ctx, "State machine - Edition-Confirmed: ValidateVersion : failed to validate version", errModel, data)
		return errModel
	}

	_, err := UpdateVersionInfo(ctx, smDS, currentVersion, versionUpdate, versionDetails)
	if err != nil {
		log.Error(ctx, "State machine - Edition-confirming: UpdateVersionInfo : failed to update the version", err, data)
		return err
	}
	return nil
}

func PublishVersion(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails,
	hasDownloads string) error {
	data := versionDetails.baseLogData()
	log.Info(ctx, "putVersion endpoint (publishVersion): beginning transition to published", data)

	// This needs to do the validation on required fields etc.
	err := models.ValidateVersion(versionUpdate)
	if err != nil {
		log.Error(ctx, "State machine - Publishing: ValidateVersion : failed to validate version", err, data)
		return err
	}

	versionUpdate, err = UpdateVersionInfo(ctx, smDS, currentVersion, versionUpdate, versionDetails)
	if err != nil {
		log.Error(ctx, "State machine - Publish: UpdateVersionInfo : failed to update the version", err, data)
		return err
	}

	if hasDownloads != trueStringified {
		log.Info(ctx, "attempting to publish edition", data)

		err = PublishEdition(ctx, smDS, versionUpdate, versionDetails, data)
		if err != nil {
			log.Error(ctx, "State machine - Publish: PublishEdition : failed to publish edition", err, data)
			return err
		}

		dsType, err := models.GetDatasetType(currentVersion.Type)
		if err != nil {
			log.Error(ctx, "State machine - Publish: GetDatasetType : failed to get dataset type", err, data)
			return err
		}

		if dsType == models.Filterable || dsType == models.CantabularFlexibleTable || dsType == models.CantabularMultivariateTable || dsType == models.CantabularTable {
			err = PublishInstance(ctx, smDS, versionUpdate, data)
			if err != nil {
				log.Error(ctx, "State machine - Publish: PublishInstance : failed to publish instance", err, data)
				return err
			}
		}

		err = PublishDataset(ctx, smDS, currentVersion, versionUpdate, versionDetails, data)
		if err != nil {
			log.Error(ctx, "State machine - Publish: PublishDataset : failed to publish dataset", err, data)
			return err
		}
	}
	return nil
}

//nolint:gocognit // Complexity is acceptable for now, refactoring can be considered later if needed
func UpdateVersionInfo(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version,
	versionDetails VersionDetails) (updatedVersion *models.Version, err error) {
	eTag := headers.IfMatchAnyETag
	if currentVersion.ETag != "" {
		eTag = currentVersion.ETag
	}

	versionNumber, err := models.ParseAndValidateVersionNumber(ctx, versionDetails.version)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: invalid version request", err)
		return nil, err
	}

	// doUpdate is an aux function that combines the existing version document with the update received in the body request,
	// then it validates the new model, and performs the update in MongoDB, passing the existing model ETag (if it exists) to be used in the query selector
	// Note that the combined version update does not mutate versionUpdate because multiple retries might generate a different value depending on the currentVersion at that point.
	var doUpdate = func() error {
		if versionUpdate != nil {
			if versionUpdate.Type == models.Static.String() {
				if _, errVersion := smDS.DataStore.Backend.UpdateVersionStatic(ctx, currentVersion, versionUpdate, eTag); errVersion != nil {
					log.Error(ctx, "putVersion endpoint: UpdateVersionStatic returned an error", err)
					return errVersion
				}
			} else {
				if _, errVersion := smDS.DataStore.Backend.UpdateVersion(ctx, currentVersion, versionUpdate, eTag); errVersion != nil {
					log.Error(ctx, "putVersion endpoint: UpdateVersion returned an error", err)
					return errVersion
				}
			}
		}

		return nil
	}

	if err := doUpdate(); err != nil {
		if err == errs.ErrDatasetNotFound {
			if versionUpdate != nil {
				if versionUpdate.Type == models.Static.String() {
					currentVersion, err = smDS.DataStore.Backend.GetVersionStatic(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
					if err != nil {
						log.Error(ctx, "putVersion endpoint: datastore.GetVersionStatic returned an error", err)
						return nil, err
					}
				} else {
					currentVersion, err = smDS.DataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
					if err != nil {
						log.Error(ctx, "putVersion endpoint: datastore.GetVersion returned an error", err)
						return nil, err
					}
				}
			}

			if err = doUpdate(); err != nil {
				log.Error(ctx, "putVersion endpoint: failed to update version document on 2nd attempt", err)
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return currentVersion, nil
}

func PublishEdition(ctx context.Context, smDS *StateMachineDatasetAPI,
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails, data log.Data) error {
	var editionDoc *models.EditionUpdate
	var versionDoc *models.Version
	var err error

	datasetType, err := smDS.DataStore.Backend.GetDatasetType(ctx, versionDetails.datasetID, true)
	if err != nil {
		log.Error(ctx, "State Machine - Publish: PublishEdition: failed to find dataset type", err, data)
		return err
	}

	if datasetType == models.Static.String() {
		version, err := strconv.Atoi(versionDetails.version)
		if err != nil {
			log.Error(ctx, "State Machine - Publish: PublishEdition: failed to convert version to integer", err, data)
			return err
		}
		versionDoc, err = smDS.DataStore.Backend.GetVersionStatic(ctx, versionDetails.datasetID, versionDetails.edition, version, "")
		if err != nil {
			log.Error(ctx, "State Machine - Publish: PublishEdition: failed to find the version we're trying to update", err, data)
			return err
		}
	} else {
		editionDoc, err = smDS.DataStore.Backend.GetEdition(ctx, versionDetails.datasetID, versionDetails.edition, "")
		if err != nil {
			log.Error(ctx, "State Machine - Publish: PublishEdition: failed to find the edition we're trying to update", err, data)
			return err
		}

		editionDoc.Next.State = models.PublishedState

		if err := editionDoc.PublishLinks(ctx, versionUpdate.Links.Version); err != nil {
			log.Error(ctx, "State Machine - Publish: PublishEdition: failed to update the edition links for the version we're trying to publish", err, data)
			return err
		}

		editionDoc.Current = editionDoc.Next
	}

	if datasetType == models.Static.String() {
		if err := smDS.DataStore.Backend.UpsertVersionStatic(ctx, versionDoc); err != nil {
			log.Error(ctx, "State Machine - Publish: PublishEdition: failed to update version during publishing", err, data)
			return err
		}
	} else {
		if err := smDS.DataStore.Backend.UpsertEdition(ctx, versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.Error(ctx, "State Machine - Publish: PublishEdition: failed to update edition during publishing", err, data)
			return err
		}
	}

	return nil
}

func PublishInstance(ctx context.Context, smDS *StateMachineDatasetAPI,
	versionUpdate *models.Version, // Called Instances in Mongo
	data log.Data) error {
	if err := smDS.DataStore.Backend.SetInstanceIsPublished(ctx, versionUpdate.ID); err != nil {
		if user := dprequest.User(ctx); user != "" {
			data[reqUser] = user
		}
		if caller := dprequest.Caller(ctx); caller != "" {
			data[reqCaller] = caller
		}
		err := errors.WithMessage(err, "State Machine - Publish: PublishInstance: failed to set instance node is_published")
		log.Error(ctx, "failed to publish instance version", err, data)
		return err
	}

	return nil
}

func PublishDataset(ctx context.Context, smDS *StateMachineDatasetAPI,
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails,
	data log.Data) error {
	// Get the dataset record
	currentDataset, err := smDS.DataStore.Backend.GetDataset(ctx, versionDetails.datasetID)
	if err != nil {
		log.Error(ctx, "State Machine: Publish: PublishDataset: unable to find dataset", err, data)
		return err
	}

	// Pass in newVersion variable to include relevant data needed for update on dataset record (e.g. links)
	if err := smDS.publishDataset(ctx, currentDataset, versionUpdate); err != nil {
		log.Error(ctx, "State Machine: Publish: PublishDataset: failed to update dataset document once version state changes to publish", err, data)
		return err
	}
	data["type"] = currentVersion.Type
	data["version_update"] = versionUpdate
	log.Info(ctx, "State Machine: Publish: PublishDataset: published version", data)

	if currentVersion.Type != models.Static.String() {
		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			// Lookup the download generator using the version document type
			t, err := models.GetDatasetType(currentVersion.Type)
			if err != nil {
				return fmt.Errorf("error getting type of version: %w", err)
			}
			generator, ok := smDS.DownloadGenerators[t]
			if !ok {
				return fmt.Errorf("no downloader available for type %s", t)
			}
			// Send Kafka message.  The generator which is used depends on the type defined in VersionDoc.
			if err := generator.Generate(ctx, versionDetails.datasetID, versionUpdate.ID, versionDetails.edition, versionDetails.version); err != nil {
				data["instance_id"] = versionUpdate.ID
				data["state"] = versionUpdate.State
				data["type"] = t.String()
				log.Error(ctx, "State Machine: Publish: PublishDataset: error while attempting to generate full dataset version downloads on version publish", err, data)
				return err
				// TODO - TECH DEBT - need to add an error event for this.  Kafka message perhaps.
			}
			log.Info(ctx, "State Machine: Publish: PublishDataset: generated full dataset version downloads:", data)
		}
	}

	return nil
}

func (smDS *StateMachineDatasetAPI) DeleteStaticVersion(ctx context.Context, datasetID, edition string, version int, filesAPIClient filesAPISDK.Clienter, token string) error {
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	// Validate edition exists for the dataset (static context)
	if err := smDS.DataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, edition, ""); err != nil {
		log.Error(ctx, "DeleteStaticVersion: edition not found or dataset missing", err, logData)
		return err
	}

	// Retrieve the version document (any state)
	versionDoc, err := smDS.DataStore.Backend.GetVersionStatic(ctx, datasetID, edition, version, "")
	if err != nil {
		log.Error(ctx, "DeleteStaticVersion: failed to find version for dataset edition", err, logData)
		return err
	}

	// Prevent deletion of published versions
	if versionDoc.State == models.PublishedState {
		log.Error(ctx, "DeleteStaticVersion: unable to delete a published version", errs.ErrDeletePublishedVersionForbidden, logData)
		return errs.ErrDeletePublishedVersionForbidden
	}

	// Delete any files associated with the version
	if versionDoc.Distributions != nil {
		for _, distribution := range *versionDoc.Distributions {
			logData["distribution_title"] = distribution.Title
			logData["distribution_download_url"] = distribution.DownloadURL

			h := filesAPISDK.Headers{Authorization: token}

			err := filesAPIClient.DeleteFile(ctx, distribution.DownloadURL, h)
			if err != nil {
				log.Error(ctx, "DeleteStaticVersion: failed to delete distribution file from files API", err, logData)
				return err
			}
			log.Info(ctx, "DeleteStaticVersion: successfully deleted distribution file from files API", logData)
		}
	}

	// Get the dataset to allow Next<-Current sync after deletion
	datasetDoc, err := smDS.DataStore.Backend.GetDataset(ctx, datasetID)
	if err != nil {
		log.Error(ctx, "DeleteStaticVersion: failed to get dataset", err, logData)
		return err
	}

	// Perform the deletion
	if err := smDS.DataStore.Backend.DeleteStaticDatasetVersion(ctx, datasetID, edition, version); err != nil {
		log.Error(ctx, "DeleteStaticVersion: failed to delete static dataset version", err, logData)
		return err
	}

	// If there is a current document, make next equal to current to retain consistency
	if datasetDoc.Current != nil {
		datasetDoc.Next = datasetDoc.Current
		if err := smDS.DataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
			log.Error(ctx, "DeleteStaticVersion: failed to update dataset after version deletion", err, logData)
			return err
		}
		log.Info(ctx, "DeleteStaticVersion: updated dataset next document to current", logData)
	}

	log.Info(ctx, "DeleteStaticVersion: successfully deleted static version", logData)
	return nil
}
