package application

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/log.go/v2/log"
)

// StaticDatasetService defines the interface for operations related to static datasets, editions, and versions.
//
// WARNING: Before calling any method, the caller must ensure the following conditions are met:
//   - The dataset exists
//   - The dataset type is "static"
//
// These checks are not performed here to avoid duplicate database calls and optimise performance.
// Handlers are expected to have already performed these checks before calling this service.
//
//go:generate moq -out mock/static_dataset_service.go -pkg mock . StaticDatasetService
type StaticDatasetService interface {
	GetVersionPublic(ctx context.Context, datasetID, editionID string, versionID int) (*models.Version, error)
	GetVersionPrivate(ctx context.Context, datasetID, editionID string, versionID int) (*models.Version, error)
}

type staticDatasetService struct {
	dataStore store.DataStore
}

// NewStaticDatasetService creates a new instance of StaticDatasetService.
func NewStaticDatasetService(dataStore store.DataStore) StaticDatasetService {
	return &staticDatasetService{
		dataStore: dataStore,
	}
}

// GetVersionPublic retrieves a published version of a dataset edition.
func (ds *staticDatasetService) GetVersionPublic(ctx context.Context, datasetID, editionID string, versionID int) (*models.Version, error) {
	if err := ds.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, editionID, models.PublishedState); err != nil {
		return nil, err
	}

	version, err := ds.dataStore.Backend.GetVersionStatic(ctx, datasetID, editionID, versionID, models.PublishedState)
	if err != nil {
		return nil, err
	}

	version.RedactPrivateFields()

	return version, nil
}

// GetVersionPrivate retrieves a version of a dataset edition, regardless of its state.
// If the editionID is not a direct match but matches a previous edition ID, it will still return the version.
func (ds *staticDatasetService) GetVersionPrivate(ctx context.Context, datasetID, editionID string, versionID int) (*models.Version, error) {
	err := ds.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, editionID, "")
	if err == nil {
		return ds.dataStore.Backend.GetVersionStatic(ctx, datasetID, editionID, versionID, "")
	}

	if !errors.Is(err, apierrors.ErrEditionNotFound) {
		return nil, err
	}

	// If the edition is not found, check if the editionID matches any previous edition IDs.
	log.Info(ctx, "edition not found, checking for previous edition IDs", log.Data{"dataset_id": datasetID, "edition_id": editionID, "version_id": versionID})

	version, err := ds.dataStore.Backend.GetVersionStaticByPreviousEditionID(ctx, datasetID, editionID, versionID)
	if err != nil {
		if errors.Is(err, apierrors.ErrVersionNotFound) {
			// If the version is not found for the previous edition ID, return the original edition not found error.
			return nil, apierrors.ErrEditionNotFound
		}
		return nil, err
	}

	log.Info(ctx, "version found using previous edition ID", log.Data{"dataset_id": datasetID, "edition_id": editionID, "matched_edition_id": version.Edition, "version_id": versionID})

	return version, nil
}
