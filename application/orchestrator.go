package application

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"

	"github.com/ONSdigital/dp-dataset-api/store"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
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

type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

type NewDatasetAPI struct {
	dataStore          store.DataStore
	downloadGenerators map[models.DatasetType]DownloadsGenerator
	stateMachine       *StateMachine
}

func Setup(ctx context.Context, router *mux.Router, dataStoreVal store.DataStore, downloadGenerators map[models.DatasetType]DownloadsGenerator, stateMachine *StateMachine) *NewDatasetAPI {
	newDS := &NewDatasetAPI{
		dataStore:          dataStoreVal,
		downloadGenerators: downloadGenerators,
		stateMachine:       stateMachine,
	}

	return newDS
}

func (v VersionDetails) baseLogData() log.Data {
	return log.Data{"dataset_id": v.datasetID, "edition": v.edition, "version": v.version}
}

func CallStateMachine(currentVersion *models.Version, versionUpdate *models.Version) {
	statesAllowed := make(map[string]State)

	statesAllowed["associated"] = Associated{}

	sm := NewStateMachine(currentVersion.State, statesAllowed[versionUpdate.State], statesAllowed, versionUpdate)

	sm.Transition()
}

func (newDS *NewDatasetAPI) AmendVersion(w http.ResponseWriter, r *http.Request) error {
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

	currentDataset, currentVersion, versionUpdate, err := newDS.UpdateVersion(ctx, r.Body, versionDetails)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return err
	}

	if vars[hasDownloads] != trueStringified {
		data["updated_state"] = versionUpdate.State
		if versionUpdate.State == models.PublishedState {
			newDS.stateMachine = NewStateMachine(currentVersion.State, castStateToState(versionUpdate.State), setAllowedStates(versionUpdate.State), versionUpdate)
			if err := newDS.stateMachine.Transition(); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return err
			}

			if err := newDS.publishVersion(ctx, currentDataset, currentVersion, versionUpdate, versionDetails); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return err
			}
		}

		if versionUpdate.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
			newDS.stateMachine = NewStateMachine(currentVersion.State, castStateToState(versionUpdate.State), setAllowedStates(versionUpdate.State), versionUpdate)
			if err := newDS.stateMachine.Transition(); err != nil {
				fmt.Println("error found")
				handleVersionAPIErr(ctx, err, w, data)
				return err
			}

			if err := newDS.associateVersion(ctx, currentVersion, versionUpdate, versionDetails); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return err
			}
		}
	}

	return nil
}

func setAllowedStates(state string) map[string]State {

	statesAllowed := make(map[string]State)
	switch s := state; s {
	case "published":
		statesAllowed["associated"] = Associated{}
	case "associated":
		statesAllowed["edition-confirmed"] = EditionConfirmed{}
	case "completed":
		statesAllowed["submitted"] = Submitted{}
	case "edition-confirmed":
		statesAllowed["completed"] = Completed{}
	case "detached":
		statesAllowed["edition-confirmed"] = EditionConfirmed{}
	case "submitted":
		statesAllowed["created"] = Created{}
	case "failed":
		statesAllowed["submitted"] = Submitted{}
	default:
		return nil
	}

	return statesAllowed
}

func castStateToState(state string) State {

	switch s := state; s {
	case "published":
		return Published{}
	case "associated":
		return Associated{}
	case "created":
		return Created{}
	case "completed":
		return Completed{}
	case "edition-confirmed":
		return EditionConfirmed{}
	case "detached":
		return Detached{}
	case "submitted":
		return Submitted{}
	case "failed":
		return Failed{}
	default:
		return nil
	}
}

func (newDS *NewDatasetAPI) associateVersion(ctx context.Context, currentVersion, versionDoc *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()
	data["type"] = currentVersion.Type
	data["version_update"] = versionDoc
	log.Info(ctx, "putVersion endpoint: associated version", data)

	associateVersionErr := func() error {
		if err := newDS.dataStore.Backend.UpdateDatasetWithAssociation(ctx, versionDetails.datasetID, versionDoc.State, versionDoc); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update dataset document after a version of a dataset has been associated with a collection", err, data)
			return err
		}

		// Get the download generator from the map, depending of the Version document type
		t, err := models.GetDatasetType(currentVersion.Type)
		if err != nil {
			return fmt.Errorf("error getting type of version: %w", err)
		}
		generator, ok := newDS.downloadGenerators[t]
		if !ok {
			return fmt.Errorf("no downloader available for type %s", t.String())
		}

		if err := generator.Generate(ctx, versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
			data["instance_id"] = versionDoc.ID
			data["state"] = versionDoc.State
			log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association", err, data)
			return err
		}
		data["type"] = t.String()
		log.Info(ctx, "putVersion endpoint (associateVersion): generated full dataset version downloads", data)
		return nil
	}()

	if associateVersionErr != nil {
		return associateVersionErr
	}

	log.Info(ctx, "associate version completed successfully", data)
	return associateVersionErr
}

func (newDS *NewDatasetAPI) UpdateVersion(ctx context.Context, body io.ReadCloser, versionDetails VersionDetails) (currentDataset *models.DatasetUpdate, currentVersion, combinedVersionUpdate *models.Version, err error) {
	data := versionDetails.baseLogData()

	reqID := ctx.Value(dprequest.RequestIdKey) // used to differentiate logs of concurrent calls to this function from different services

	versionNumber, err := models.ParseAndValidateVersionNumber(ctx, versionDetails.version)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: invalid version request", err, data)
		return nil, nil, nil, err
	}

	// reads http header and creates struct for new versionNumber
	versionUpdate, err := models.CreateVersion(body, versionDetails.datasetID)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: failed to model version resource based on request", err, data)
		return nil, nil, nil, errs.ErrUnableToParseJSON
	}

	currentDataset, err = newDS.dataStore.Backend.GetDataset(ctx, versionDetails.datasetID)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: datastore.getDataset returned an error", err, data)
		return nil, nil, nil, err
	}

	if err = newDS.dataStore.Backend.CheckEditionExists(ctx, versionDetails.datasetID, versionDetails.edition, ""); err != nil {
		log.Error(ctx, "putVersion endpoint: failed to find edition of dataset", err, data)
		return nil, nil, nil, err
	}

	currentVersion, err = newDS.dataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
	if err != nil {
		log.Error(ctx, "putVersion endpoint: datastore.GetVersion returned an error", err, data)
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
			log.Error(ctx, "putVersion endpoint: failed validation check for version update", err)
			return err
		}

		newDS.stateMachine = NewStateMachine(currentVersion.State, castStateToState(versionUpdate.State), setAllowedStates(versionUpdate.State), versionUpdate)

		err2 := newDS.stateMachine.Transition()
		if err2 != nil {
			fmt.Println("error found")
			return err2
		}

		eTag := headers.IfMatchAnyETag
		if currentVersion.ETag != "" {
			eTag = currentVersion.ETag
		}

		if _, err := newDS.dataStore.Backend.UpdateVersion(ctx, currentVersion, combinedVersionUpdate, eTag); err != nil {
			return err
		}

		return nil
	}

	// acquire instance lock to prevent race conditions on instance collection
	lockID, err := newDS.dataStore.Backend.AcquireInstanceLock(ctx, currentVersion.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		newDS.dataStore.Backend.UnlockInstance(ctx, lockID)
	}()

	// Try to perform the update. If there was a race condition and another caller performed the update
	// before we could acquire the lock, this will result in the ETag being changed
	// and the update failing with ErrDatasetNotFound.
	// In this scenario we re-try the get + update before releasing the lock.
	// Note that the lock and ETag will also protect against race conditions with instance endpoints,
	// which may also modify the same instance collection in the database.
	if err := doUpdate(); err != nil {
		if err == errs.ErrDatasetNotFound {
			log.Info(ctx, "instance document in database corresponding to dataset version was modified before the lock was acquired, retrying...", data)
			currentVersion, err = newDS.dataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
			if err != nil {
				log.Error(ctx, "putVersion endpoint: datastore.GetVersion returned an error", err, data)
				return nil, nil, nil, err
			}

			if err = doUpdate(); err != nil {
				log.Error(ctx, "putVersion endpoint: failed to update version document on 2nd attempt", err, data)
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

func (newDS *NewDatasetAPI) publishVersion(
	ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails, // Struct holding URL Params.
) error {
	data := versionDetails.baseLogData()
	log.Info(ctx, "attempting to publish version", data)
	err := func() error {
		editionDoc, err := newDS.dataStore.Backend.GetEdition(ctx, versionDetails.datasetID, versionDetails.edition, "")
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

		if err := newDS.dataStore.Backend.UpsertEdition(ctx, versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update edition during publishing", err, data)
			return err
		}

		if err := newDS.dataStore.Backend.SetInstanceIsPublished(ctx, versionUpdate.ID); err != nil {
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
		if err := newDS.publishDataset(ctx, currentDataset, versionUpdate); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update dataset document once version state changes to publish", err, data)
			return err
		}
		data["type"] = currentVersion.Type
		data["version_update"] = versionUpdate
		log.Info(ctx, "putVersion endpoint: published version", data)

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			// Lookup the download generator using the version document type
			t, err := models.GetDatasetType(currentVersion.Type)
			if err != nil {
				return fmt.Errorf("error getting type of version: %w", err)
			}
			generator, ok := newDS.downloadGenerators[t]
			if !ok {
				return fmt.Errorf("no downloader available for type %s", t)
			}
			// Send Kafka message.  The generator which is used depends on the type defined in VersionDoc.
			if err := generator.Generate(ctx, versionDetails.datasetID, versionUpdate.ID, versionDetails.edition, versionDetails.version); err != nil {
				data["instance_id"] = versionUpdate.ID
				data["state"] = versionUpdate.State
				data["type"] = t.String()
				log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version publish", err, data)
				return err
				// TODO - TECH DEBT - need to add an error event for this.  Kafka message perhaps.
			}
			log.Info(ctx, "putVersion endpoint (publishVersions): generated full dataset version downloads:", data)
		}
		return nil
	}()

	if err != nil {
		return err
	}

	log.Info(ctx, "publish version completed successfully", data)
	return nil
}

func (newDS *NewDatasetAPI) publishDataset(ctx context.Context, currentDataset *models.DatasetUpdate, version *models.Version) error {
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

	if err := newDS.dataStore.Backend.UpsertDataset(ctx, currentDataset.ID, newDataset); err != nil {
		log.Error(ctx, "unable to update dataset", err, log.Data{"dataset_id": currentDataset.ID})
		return err
	}

	return nil
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
