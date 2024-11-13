package application

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-api-clients-go/headers"
	"github.com/ONSdigital/dp-dataset-api/models"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

type Created struct{}

func (g Created) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Creating")
	return nil
}
func (g Created) String() string {
	return "created"
}

type Submitted struct{}

func (g Submitted) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Submitting")
	return nil
}
func (g Submitted) String() string {
	return "submitted"
}

type Completed struct{}

func (g Completed) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("completing")
	return nil
}
func (g Completed) String() string {
	return "completed"
}

type Failed struct{}

func (g Failed) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Failing")
	return nil
}
func (g Failed) String() string {
	return "failed"
}

type EditionConfirmed struct{}

func (g EditionConfirmed) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Edition confirming")
	return nil
}
func (g EditionConfirmed) String() string {
	return "edition-confirmed"
}

type Detached struct{}

func (g Detached) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Detaching")
	return nil
}
func (g Detached) String() string {
	return "detached"
}

type Associated struct{}

func (g Associated) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Associating")
	err := models.ValidateVersion(combinedVersionUpdate)
	if err != nil {
		fmt.Println("Validation Failed")
		fmt.Println(err)
		return err
	} else {
		fmt.Println("Validation passed, continue")
		return nil
	}
}
func (g Associated) String() string {
	return "associated"
}

type Published struct{}

func (g Published) Enter(combinedVersionUpdate *models.Version, l *StateMachine, smDS *StateMachineDatasetAPI, ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails) error {
	fmt.Println("Publishing")

	// This needs to do the validation on required fields etc.
	errModel := models.ValidateVersion(combinedVersionUpdate)
	if errModel != nil {
		fmt.Println("Validation Failed")
		fmt.Println(errModel)
		return errModel
	} else {
		fmt.Println("Validation passed, continue")
		eTag := headers.IfMatchAnyETag
		if currentVersion.ETag != "" {
			eTag = currentVersion.ETag
		}

		lockID, error := smDS.dataStore.Backend.AcquireInstanceLock(ctx, currentVersion.ID)
		if error != nil {
			return error
		}
		defer func() {
			smDS.dataStore.Backend.UnlockInstance(ctx, lockID)
		}()

		if _, errVersion := smDS.dataStore.Backend.UpdateVersion(ctx, currentVersion, combinedVersionUpdate, eTag); errVersion != nil {
			return errVersion
		}

		data := versionDetails.baseLogData()
		log.Info(ctx, "attempting to publish version", data)
		//errEdition := func() error {
		editionDoc, err := smDS.dataStore.Backend.GetEdition(ctx, versionDetails.datasetID, versionDetails.edition, "")
		if err != nil {
			log.Error(ctx, "publishVersion: failed to find the edition we're trying to update", err, data)
			return err
		}

		editionDoc.Next.State = models.PublishedState

		if err := editionDoc.PublishLinks(ctx, versionUpdate.Links.Version); err != nil {
			log.Error(ctx, "publishVersion: failed to update the edition links for the version we're trying to publish", err, data)
			return err
		}

		editionDoc.Current = editionDoc.Next

		if err := smDS.dataStore.Backend.UpsertEdition(ctx, versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.Error(ctx, "publishVersion: failed to update edition during publishing", err, data)
			return err
		}

		if err := smDS.dataStore.Backend.SetInstanceIsPublished(ctx, versionUpdate.ID); err != nil {
			if user := dprequest.User(ctx); user != "" {
				data[reqUser] = user
			}
			if caller := dprequest.Caller(ctx); caller != "" {
				data[reqCaller] = caller
			}
			err := errors.WithMessage(err, "publishVersion: failed to set instance node is_published")
			log.Error(ctx, "failed to publish instance version", err, data)
			return err
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := smDS.publishDataset(ctx, currentDataset, versionUpdate); err != nil {
			log.Error(ctx, "publishVersion: failed to update dataset document once version state changes to publish", err, data)
			return err
		}
		data["type"] = currentVersion.Type
		data["version_update"] = versionUpdate
		log.Info(ctx, "publishVersion: published version", data)

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			// Lookup the download generator using the version document type
			t, err := models.GetDatasetType(currentVersion.Type)
			if err != nil {
				return fmt.Errorf("error getting type of version: %w", err)
			}
			generator, ok := smDS.downloadGenerators[t]
			if !ok {
				return fmt.Errorf("no downloader available for type %s", t)
			}
			// Send Kafka message.  The generator which is used depends on the type defined in VersionDoc.
			if err := generator.Generate(ctx, versionDetails.datasetID, versionUpdate.ID, versionDetails.edition, versionDetails.version); err != nil {
				data["instance_id"] = versionUpdate.ID
				data["state"] = versionUpdate.State
				data["type"] = t.String()
				log.Error(ctx, "publishVersion: error while attempting to generate full dataset version downloads on version publish", err, data)
				return err
				// TODO - TECH DEBT - need to add an error event for this.  Kafka message perhaps.
			}
			log.Info(ctx, "publishVersion: generated full dataset version downloads:", data)
		}

		return nil

	}
}

func (g Published) String() string {
	return "published"
}
