package utils

import (
	"context"
	"net/url"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-net/v3/links"
	"github.com/ONSdigital/log.go/v2/log"
)

func RewriteDatasetsWithAuth(ctx context.Context, results []*models.DatasetUpdate, datasetLinksBuilder *links.Builder) ([]*models.DatasetUpdate, error) {
	if len(results) == 0 {
		return results, nil
	}

	items := []*models.DatasetUpdate{}
	for _, item := range results {
		if item.Current != nil {
			err := RewriteDatasetLinks(ctx, item.Current.Links, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'current' links", err)
				return nil, err
			}
		}

		if item.Next != nil {
			err := RewriteDatasetLinks(ctx, item.Next.Links, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'next' links", err)
				return nil, err
			}
		}
		items = append(items, item)
	}
	return items, nil
}

func RewriteDatasetsWithoutAuth(ctx context.Context, results []*models.DatasetUpdate, datasetLinksBuilder *links.Builder) ([]*models.Dataset, error) {
	if len(results) == 0 {
		return []*models.Dataset{}, nil
	}

	items := []*models.Dataset{}
	for _, item := range results {
		if item.Current != nil {
			err := RewriteDatasetLinks(ctx, item.Current.Links, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'current' links", err)
				return nil, err
			}
			item.Current.ID = item.ID
			items = append(items, item.Current)
		}
	}
	return items, nil
}

func RewriteDatasetWithAuth(ctx context.Context, dataset *models.DatasetUpdate, datasetLinksBuilder *links.Builder) (*models.DatasetUpdate, error) {
	if dataset == nil {
		log.Info(ctx, "getDataset endpoint: dataset is empty")
		return nil, errs.ErrDatasetNotFound
	}

	if dataset.Current == nil && dataset.Next == nil {
		log.Info(ctx, "getDataset endpoint: published or unpublished dataset not found")
		return nil, errs.ErrDatasetNotFound
	}

	log.Info(ctx, "getDataset endpoint: caller authorised returning dataset current sub document", log.Data{"dataset_id": dataset.ID})

	if dataset.Current != nil && dataset.Current.Type == models.Static.String() && dataset.Current.Topics == nil {
		dataset.Current.Topics = []string{}
	}

	if dataset.Current != nil && dataset.Current.Type != models.Static.String() {
		dataset.Current.Topics = nil
	}

	if dataset.Current != nil {
		err := RewriteDatasetLinks(ctx, dataset.Current.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' links", err)
			return nil, err
		}
	}

	if dataset.Next != nil && dataset.Next.Type == models.Static.String() && dataset.Next.Topics == nil {
		dataset.Next.Topics = []string{}
	}

	if dataset.Next != nil && dataset.Next.Type != models.Static.String() {
		dataset.Next.Topics = nil
	}

	if dataset.Next != nil {
		err := RewriteDatasetLinks(ctx, dataset.Next.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'next' links", err)
			return nil, err
		}
	}

	return dataset, nil
}

func RewriteDatasetWithoutAuth(ctx context.Context, dataset *models.DatasetUpdate, datasetLinksBuilder *links.Builder) (*models.Dataset, error) {
	if dataset == nil {
		log.Info(ctx, "getDataset endpoint: dataset is empty")
		return nil, errs.ErrDatasetNotFound
	}

	if dataset.Current == nil {
		log.Info(ctx, "getDataset endpoint: published dataset not found", log.Data{"dataset_id": dataset.ID})
		return nil, errs.ErrDatasetNotFound
	}

	datasetResponse := &models.Dataset{}
	log.Info(ctx, "getDataset endpoint: caller not authorised returning dataset", log.Data{"dataset_id": dataset.ID})

	dataset.Current.ID = dataset.ID

	if dataset.Current.Type != models.Static.String() {
		dataset.Current.Topics = nil
	}

	datasetResponse = dataset.Current
	err := RewriteDatasetLinks(ctx, datasetResponse.Links, datasetLinksBuilder)
	if err != nil {
		log.Error(ctx, "failed to rewrite 'current' links", err)
		return nil, err
	}

	return datasetResponse, nil
}
func RewriteDatasetLinks(ctx context.Context, oldLinks *models.DatasetLinks, datasetLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	prevLinks := []*models.LinkObject{
		oldLinks.Editions,
		oldLinks.LatestVersion,
		oldLinks.Self,
		oldLinks.Taxonomy,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = datasetLinksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteDimensions(ctx context.Context, results []models.Dimension, datasetLinksBuilder, codeListLinksBuilder *links.Builder) ([]models.Dimension, error) {
	if len(results) == 0 {
		return results, nil
	}

	items := []models.Dimension{}

	var err error

	for i := range results {
		item := &results[i]
		if item.HRef != "" {
			item.HRef, err = codeListLinksBuilder.BuildLink(item.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": item.HRef})
				return nil, err
			}
		}

		err := RewriteDimensionLinks(ctx, &item.Links, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension links", err)
			return nil, err
		}
		items = append(items, *item)
	}
	return items, nil
}

func RewriteDimensionLinks(ctx context.Context, oldLinks *models.DimensionLink, datasetLinksBuilder, codeListLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	var err error

	if oldLinks.CodeList.HRef != "" {
		oldLinks.CodeList.HRef, err = codeListLinksBuilder.BuildLink(oldLinks.CodeList.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite codeList link", err, log.Data{"link": oldLinks.CodeList.HRef})
			return err
		}
	}

	if oldLinks.Options.HRef != "" {
		oldLinks.Options.HRef, err = datasetLinksBuilder.BuildLink(oldLinks.Options.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite options link", err, log.Data{"link": oldLinks.Options.HRef})
			return err
		}
	}

	if oldLinks.Version.HRef != "" {
		oldLinks.Version.HRef, err = datasetLinksBuilder.BuildLink(oldLinks.Version.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite version link", err, log.Data{"link": oldLinks.Version.HRef})
			return err
		}
	}

	return nil
}

func RewritePublicDimensionOptions(ctx context.Context, results []*models.PublicDimensionOption, datasetLinksBuilder, codeListLinksBuilder *links.Builder) ([]*models.PublicDimensionOption, error) {
	if len(results) == 0 {
		return results, nil
	}

	items := []*models.PublicDimensionOption{}
	for _, item := range results {
		err := RewriteDimensionOptionLinks(ctx, &item.Links, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite public dimension option links", err)
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func RewriteDimensionOptions(ctx context.Context, results []*models.DimensionOption, datasetLinksBuilder, codeListLinksBuilder *links.Builder) error {
	if len(results) == 0 {
		return nil
	}

	var err error
	for _, item := range results {
		err = RewriteDimensionOptionLinks(ctx, &item.Links, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension option links", err)
			return err
		}
	}
	return nil
}

func RewriteDimensionOptionLinks(ctx context.Context, oldLinks *models.DimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	var err error

	if oldLinks.Code.HRef != "" {
		oldLinks.Code.HRef, err = codeListLinksBuilder.BuildLink(oldLinks.Code.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite code link", err, log.Data{"link": oldLinks.Code.HRef})
			return err
		}
	}

	if oldLinks.CodeList.HRef != "" {
		oldLinks.CodeList.HRef, err = codeListLinksBuilder.BuildLink(oldLinks.CodeList.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite codeList link", err, log.Data{"link": oldLinks.CodeList.HRef})
			return err
		}
	}

	if oldLinks.Version.HRef != "" {
		oldLinks.Version.HRef, err = datasetLinksBuilder.BuildLink(oldLinks.Version.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite version link", err, log.Data{"link": oldLinks.Version.HRef})
			return err
		}
	}

	return nil
}

func RewriteEditionsWithAuth(ctx context.Context, results []*models.EditionUpdate, datasetLinksBuilder *links.Builder, downloadServiceURL *url.URL) ([]*models.EditionUpdate, error) {
	if len(results) == 0 {
		return results, nil
	}

	items := []*models.EditionUpdate{}
	for _, item := range results {
		if item.Current != nil {
			err := RewriteEditionLinks(ctx, item.Current.Links, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'current' links", err)
				return nil, err
			}

			item.Current.Distributions, err = RewriteDistributions(ctx, item.Current.Distributions, downloadServiceURL)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'current' distributions DownloadURLs", err)
				return nil, err
			}
		}

		if item.Next != nil {
			err := RewriteEditionLinks(ctx, item.Next.Links, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'next' links", err)
				return nil, err
			}

			item.Next.Distributions, err = RewriteDistributions(ctx, item.Next.Distributions, downloadServiceURL)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'next' distributions DownloadURLs", err)
				return nil, err
			}
		}
		items = append(items, item)
	}
	return items, nil
}

func RewriteEditionsWithoutAuth(ctx context.Context, results []*models.EditionUpdate, datasetLinksBuilder *links.Builder, downloadServiceURL *url.URL) ([]*models.Edition, error) {
	if len(results) == 0 {
		return []*models.Edition{}, nil
	}

	items := []*models.Edition{}
	for _, item := range results {
		if item.Current == nil {
			continue
		}

		err := RewriteEditionLinks(ctx, item.Current.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' links", err)
			return nil, err
		}

		item.Current.Distributions, err = RewriteDistributions(ctx, item.Current.Distributions, downloadServiceURL)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' distributions DownloadURLs", err)
			return nil, err
		}

		item.Current.ID = item.ID
		items = append(items, item.Current)
	}
	return items, nil
}

func RewriteEditionWithAuth(ctx context.Context, edition *models.EditionUpdate, datasetLinksBuilder *links.Builder, downloadServiceURL *url.URL) (*models.EditionUpdate, error) {
	if edition == nil {
		log.Info(ctx, "getEdition endpoint: published or unpublished edition not found")
		return nil, errs.ErrEditionNotFound
	}

	if edition.Current != nil {
		err := RewriteEditionLinks(ctx, edition.Current.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' links", err)
			return nil, err
		}
		edition.Current.Distributions, err = RewriteDistributions(ctx, edition.Current.Distributions, downloadServiceURL)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' distributions DownloadURLs", err)
			return nil, err
		}
	}

	if edition.Next != nil {
		err := RewriteEditionLinks(ctx, edition.Next.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'next' links", err)
			return nil, err
		}
		edition.Next.Distributions, err = RewriteDistributions(ctx, edition.Next.Distributions, downloadServiceURL)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'next' distributions DownloadURLs", err)
			return nil, err
		}
	}

	return edition, nil
}

func RewriteEditionWithoutAuth(ctx context.Context, edition *models.EditionUpdate, datasetLinksBuilder *links.Builder, downloadServiceURL *url.URL) (*models.Edition, error) {
	if edition == nil {
		log.Info(ctx, "getEdition endpoint: published edition not found")
		return nil, errs.ErrEditionNotFound
	}

	editionResponse := &models.Edition{}
	if edition.Current == nil {
		log.Info(ctx, "getEdition endpoint: published edition not found")
		return nil, nil
	}
	log.Info(ctx, "getEdition endpoint: caller not authorised returning edition", log.Data{"edition_id": edition.ID})

	edition.Current.ID = edition.ID
	editionResponse = edition.Current
	err := RewriteEditionLinks(ctx, editionResponse.Links, datasetLinksBuilder)
	if err != nil {
		log.Error(ctx, "failed to rewrite 'current' links", err)
		return nil, err
	}
	editionResponse.Distributions, err = RewriteDistributions(ctx, editionResponse.Distributions, downloadServiceURL)
	if err != nil {
		log.Error(ctx, "failed to rewrite 'current' distributions DownloadURLs", err)
		return nil, err
	}

	return editionResponse, nil
}

func RewriteEditionLinks(ctx context.Context, oldLinks *models.EditionUpdateLinks, datasetLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	prevLinks := []*models.LinkObject{
		oldLinks.Dataset,
		oldLinks.LatestVersion,
		oldLinks.Self,
		oldLinks.Versions,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = datasetLinksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteMetadataLinks(ctx context.Context, oldLinks *models.MetadataLinks, datasetLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	var err error

	if oldLinks.Self != nil && oldLinks.Self.HRef != "" {
		oldLinks.Self.HRef, err = datasetLinksBuilder.BuildLink(oldLinks.Self.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite self link", err, log.Data{"link": oldLinks.Self.HRef})
			return err
		}
	}

	if oldLinks.Version != nil && oldLinks.Version.HRef != "" {
		oldLinks.Version.HRef, err = datasetLinksBuilder.BuildLink(oldLinks.Version.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite version link", err, log.Data{"link": oldLinks.Version.HRef})
			return err
		}
	}

	return nil
}

func RewriteVersions(ctx context.Context, results []models.Version, datasetLinksBuilder, codeListLinksBuilder *links.Builder, downloadServiceURL *url.URL) ([]models.Version, error) {
	if len(results) == 0 {
		return results, nil
	}

	items := []models.Version{}

	var err error

	for i := range results {
		item := &results[i]
		item.Dimensions, err = RewriteDimensions(ctx, item.Dimensions, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension links", err)
			return nil, err
		}

		err = RewriteVersionLinks(ctx, item.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite version links", err)
			return nil, err
		}

		err = RewriteDownloadLinks(ctx, item.Downloads, downloadServiceURL)
		if err != nil {
			log.Error(ctx, "failed to rewrite download links", err)
			return nil, err
		}

		item.Distributions, err = RewriteDistributions(ctx, item.Distributions, downloadServiceURL)
		if err != nil {
			log.Error(ctx, "failed to rewrite distributions DownloadURLs", err)
			return nil, err
		}

		items = append(items, *item)
	}

	return items, nil
}

func RewriteVersionLinks(ctx context.Context, oldLinks *models.VersionLinks, datasetLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	prevLinks := []*models.LinkObject{
		oldLinks.Dataset,
		oldLinks.Dimensions,
		oldLinks.Edition,
		oldLinks.Self,
		oldLinks.Version,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = datasetLinksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}

	return nil
}

func RewriteInstances(ctx context.Context, results []*models.Instance, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder *links.Builder, downloadServiceURL *url.URL) error {
	if len(results) == 0 {
		return nil
	}

	var err error

	for _, item := range results {
		item.Dimensions, err = RewriteDimensions(ctx, item.Dimensions, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension links", err)
			return err
		}

		err = RewriteInstanceLinks(ctx, item.Links, datasetLinksBuilder, importLinksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite instance links", err)
			return err
		}

		err = RewriteDownloadLinks(ctx, item.Downloads, downloadServiceURL)
		if err != nil {
			log.Error(ctx, "failed to rewrite download links", err)
			return err
		}
	}
	return nil
}

func RewriteInstanceLinks(ctx context.Context, oldLinks *models.InstanceLinks, datasetLinksBuilder, importLinksBuilder *links.Builder) error {
	if oldLinks == nil {
		return nil
	}

	prevLinks := []*models.LinkObject{
		oldLinks.Dataset,
		oldLinks.Dimensions,
		oldLinks.Edition,
		oldLinks.Self,
		oldLinks.Version,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = datasetLinksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}

	if oldLinks.Job != nil && oldLinks.Job.HRef != "" {
		oldLinks.Job.HRef, err = importLinksBuilder.BuildLink(oldLinks.Job.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite job link", err, log.Data{"link": oldLinks.Job.HRef})
			return err
		}
	}

	return nil
}

func RewriteDownloadLinks(ctx context.Context, oldLinks *models.DownloadList, downloadServiceURL *url.URL) error {
	if oldLinks == nil {
		return nil
	}

	prevLinks := []*models.DownloadObject{
		oldLinks.CSV,
		oldLinks.CSVW,
		oldLinks.TXT,
		oldLinks.XLS,
		oldLinks.XLSX,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = links.BuildDownloadLink(link.HRef, downloadServiceURL)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteDistributions(ctx context.Context, results *[]models.Distribution, downloadServiceURL *url.URL) (*[]models.Distribution, error) {
	if results == nil || len(*results) == 0 {
		return results, nil
	}

	items := &[]models.Distribution{}

	var err error

	for _, item := range *results {
		if item.DownloadURL != "" {
			item.DownloadURL, err = links.BuildDownloadFilesLink(item.DownloadURL, downloadServiceURL)
			if err != nil {
				log.Error(ctx, "failed to rewrite DownloadURL", err)
				return nil, err
			}
			*items = append(*items, item)
		}
	}
	return items, nil
}
