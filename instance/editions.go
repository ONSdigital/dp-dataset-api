package instance

import (
	"context"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"
)

func (s *Store) confirmEdition(ctx context.Context, datasetID, edition, instanceID string) (*models.EditionUpdate, error) {
	logData := log.Data{"dataset_id": datasetID, "instance_id": instanceID, "edition": edition}
	var editionDoc *models.EditionUpdate
	var action string
	var err error

	if editionDoc, action, err = func() (*models.EditionUpdate, string, error) {
		log.Info(ctx, "confirm edition: getting edition", logData)

		editionDoc, err := s.GetEdition(ctx, datasetID, edition, "")
		if err != nil {
			if err != errs.ErrEditionNotFound {
				log.Error(ctx, "confirm edition: failed to confirm edition", err, logData)
				return nil, action, err
			}

			log.Info(ctx, "confirm edition: edition not found, creating", logData)
			editionDoc, err = models.CreateEdition(s.Host, datasetID, edition)
			if err != nil {
				return nil, action, err
			}

			log.Info(ctx, "confirm edition: created new edition", logData)
		} else {
			// TODO - feature flag. Will need removing eventually.
			if s.EnableDetachDataset {
				// Abort if a new/next version is already in flight
				if editionDoc.Current != nil && editionDoc.Next != nil && editionDoc.Current.Links.LatestVersion.ID != editionDoc.Next.Links.LatestVersion.ID {
					log.Info(ctx, "confirm edition: there was an attempted skip of versioning sequence. Aborting edition update", logData)
					return nil, action, errs.ErrVersionAlreadyExists
				}
			}

			log.Info(ctx, "confirm edition: edition found, updating", logData)

			if err = editionDoc.UpdateLinks(ctx, s.Host); err != nil {
				log.Error(ctx, "confirm edition: unable to update edition links", err, logData)
				return nil, action, err
			}
		}

		editionDoc.Next.State = models.EditionConfirmedState

		if err = s.UpsertEdition(ctx, datasetID, edition, editionDoc); err != nil {
			log.Error(ctx, "confirm edition: store.UpsertEdition returned an error", err, logData)
			return nil, action, err
		}

		return editionDoc, action, nil
	}(); err != nil {
		return nil, err
	}

	log.Info(ctx, "confirm edition: created/updated edition", logData)
	return editionDoc, nil
}
