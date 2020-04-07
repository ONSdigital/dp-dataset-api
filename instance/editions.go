package instance

import (
	"context"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/log.go/log"
)

func (s *Store) confirmEdition(ctx context.Context, datasetID, edition, instanceID string) (*models.EditionUpdate, error) {
	auditParams := common.Params{"dataset_id": datasetID, "instance_id": instanceID, "edition": edition}
	logData := audit.ToLogData(auditParams)

	var editionDoc *models.EditionUpdate
	var action string
	var err error

	if editionDoc, action, err = func() (*models.EditionUpdate, string, error) {

		log.Event(ctx, "confirm edition: getting edition", log.INFO, logData)
		editionDoc, err := s.GetEdition(datasetID, edition, "")
		if err != nil {
			if err != errs.ErrEditionNotFound {
				log.Event(ctx, "confirm edition: failed to confirm edition", log.ERROR, log.Error(err), logData)
				return nil, action, err
			}

			log.Event(ctx, "confirm edition: edition not found, creating", log.INFO, logData)
			action = CreateEditionAction
			if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
				return nil, action, auditErr
			}

			editionDoc, err = models.CreateEdition(s.Host, datasetID, edition)
			if err != nil {
				return nil, action, err
			}

			log.Event(ctx, "confirm edition: created new edition", log.INFO, logData)
		} else {

			action = UpdateEditionAction

			// TODO - feature flag. Will need removing eventually.
			if s.EnableDetachDataset {
				// Abort if a new/next version is already in flight
				if editionDoc.Current == nil || editionDoc.Current.Links.LatestVersion.ID != editionDoc.Next.Links.LatestVersion.ID {
					log.Event(ctx, "confirm edition: there was an attempted skip of versioning sequence. Aborting edition update", log.INFO, logData)
					return nil, action, errs.ErrVersionAlreadyExists
				}
			}

			log.Event(ctx, "confirm edition: edition found, updating", log.INFO, logData)
			if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
				return nil, action, auditErr
			}

			if err = editionDoc.UpdateLinks(ctx, s.Host); err != nil {
				log.Event(ctx, "confirm edition: unable to update edition links", log.ERROR, log.Error(err), logData)
				return nil, action, err
			}
		}

		editionDoc.Next.State = models.EditionConfirmedState

		if err = s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
			log.Event(ctx, "confirm edition: store.UpsertEdition returned an error", log.ERROR, log.Error(err), logData)
			return nil, action, err
		}

		return editionDoc, action, nil
	}(); err != nil {
		if auditErr := s.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
			return nil, auditErr
		}
		return nil, err
	}

	s.Auditor.Record(ctx, action, audit.Successful, auditParams)
	log.Event(ctx, "confirm edition: created/updated edition", logData)
	return editionDoc, nil
}
