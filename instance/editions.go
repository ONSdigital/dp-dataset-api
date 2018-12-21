package instance

import (
	"context"
	"fmt"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
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

			editionDoc = s.createEdition(datasetID, edition)
			log.Debug("created new edition", logData)
		} else {
			log.Debug("edition found, updating", logData)
			action = UpdateEditionAction
			if auditErr := s.Auditor.Record(ctx, action, audit.Attempted, auditParams); auditErr != nil {
				return nil, action, auditErr
			}

			if err = editionDoc.UpdateLinks(s.Host); err != nil {
				logData["version"] = editionDoc.Next.Links.LatestVersion.ID
				log.ErrorCtx(ctx, errors.WithMessage(err, "unable to retrieve latest version"), logData)
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

func (s *Store) createEdition(datasetID, edition string) *models.EditionUpdate {
	return &models.EditionUpdate{
		ID: uuid.NewV4().String(),
		Next: &models.Edition{
			Edition: edition,
			State:   models.EditionConfirmedState,
			Links: &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					ID:   datasetID,
					HRef: fmt.Sprintf("%s/datasets/%s", s.Host, datasetID),
				},
				Self: &models.LinkObject{
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", s.Host, datasetID, edition),
				},
				Versions: &models.LinkObject{
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions", s.Host, datasetID, edition),
				},
				LatestVersion: &models.LinkObject{
					ID:   "1",
					HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/1", s.Host, datasetID, edition),
				},
			},
		},
	}
}
