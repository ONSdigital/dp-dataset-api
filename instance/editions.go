package instance

import (
	"context"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
)

func (s *Store) confirmEdition(ctx context.Context, datasetID, edition, instanceID string) (*models.EditionUpdate, error) {
	auditParams := common.Params{"dataset_id": datasetID, "instance_id": instanceID, "edition": edition}
	logData := audit.ToLogData(auditParams)

	var editionDoc *models.EditionUpdate
	var action string
	var err error

	if editionDoc, action, err = func() (*models.EditionUpdate, string, error) {

		log.Debug("getting edition", logData)
		editionDoc, err := s.GetEdition(datasetID, edition, "")
		if err != nil {
			if err != errs.ErrEditionNotFound {
				log.ErrorCtx(ctx, err, logData)
				return nil, action, err
			}

			log.Debug("edition not found, creating", logData)
			action = CreateEditionAction
			if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
				return nil, action, auditErr
			}

			editionDoc = models.CreateEdition(s.Host, datasetID, edition)
			log.Debug("created new edition", logData)
		} else {

			action = UpdateEditionAction

			// Abort if a new/next version is already in flight
			if editionDoc.Current == nil || editionDoc.Current.Links.LatestVersion.ID != editionDoc.Next.Links.LatestVersion.ID {
				log.InfoCtx(ctx, "there was an attempted skip of versioning sequence. Aborting edition update", logData)
				return nil, action, errs.ErrVersionAlreadyExists
			}

			log.Debug("edition found, updating", logData)
			if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
				return nil, action, auditErr
			}

			if err = editionDoc.UpdateLinks(s.Host); err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "unable to update edition links"), logData)
				return nil, action, err
			}
		}

		editionDoc.Next.State = models.EditionConfirmedState

		if err = s.UpsertEdition(datasetID, edition, editionDoc); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "confirm edition: store.UpsertEdition returned an error"), logData)
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
	log.InfoCtx(ctx, "instance update: created/updated edition", logData)
	return editionDoc, nil
}
