package utils

import (
	"github.com/ONSdigital/dp-dataset-api/models"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
)

func MapVersionToEdition(version *models.Version) *models.Edition {
	edition := &models.Edition{
		DatasetID:    version.DatasetID,
		Edition:      version.Edition,
		EditionTitle: version.EditionTitle,
		ReleaseDate:  version.ReleaseDate,
		Links: &models.EditionUpdateLinks{
			Dataset: &models.LinkObject{
				HRef: version.Links.Dataset.HRef,
				ID:   version.Links.Dataset.ID,
			},
			LatestVersion: &models.LinkObject{
				HRef: version.Links.Version.HRef,
				ID:   version.Links.Version.ID,
			},
			Self: &models.LinkObject{
				HRef: version.Links.Edition.HRef,
				ID:   version.Links.Edition.ID,
			},
			Versions: &models.LinkObject{
				HRef: version.Links.Edition.HRef + "/versions",
			},
		},
		State:              version.State,
		Version:            version.Version,
		LastUpdated:        version.LastUpdated,
		Alerts:             version.Alerts,
		UsageNotes:         version.UsageNotes,
		Distributions:      version.Distributions,
		QualityDesignation: version.QualityDesignation,
	}

	return edition
}

func MapVersionsToEditionUpdate(publishedVersion, unpublishedVersion *models.Version) (*models.EditionUpdate, error) {
	var edition *models.EditionUpdate

	switch {
	case publishedVersion == nil && unpublishedVersion == nil:
		return nil, errs.ErrVersionNotFound
	case publishedVersion != nil && unpublishedVersion != nil:
		edition = &models.EditionUpdate{
			Current: MapVersionToEdition(publishedVersion),
			Next:    MapVersionToEdition(unpublishedVersion),
		}
	case publishedVersion != nil && unpublishedVersion == nil:
		edition = &models.EditionUpdate{
			Current: MapVersionToEdition(publishedVersion),
			Next:    MapVersionToEdition(publishedVersion),
		}
	case publishedVersion == nil && unpublishedVersion != nil:
		edition = &models.EditionUpdate{
			Next: MapVersionToEdition(unpublishedVersion),
		}
	}

	return edition, nil
}
