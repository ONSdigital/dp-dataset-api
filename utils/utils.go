package utils

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-net/v2/links"
	"github.com/ONSdigital/log.go/v2/log"
)

// ValidatePositiveInt obtains the positive int value of query var defined by the provided varKey
func ValidatePositiveInt(parameter string) (val int, err error) {
	val, err = strconv.Atoi(parameter)
	if err != nil {
		return -1, errs.ErrInvalidQueryParameter
	}
	if val < 0 {
		return -1, errs.ErrInvalidQueryParameter
	}
	return val, nil
}

// GetQueryParamListValues obtains a list of strings from the provided queryVars,
// by parsing all values with key 'varKey' and splitting the values by commas, if they contain commas.
// Up to maxNumItems values are allowed in total.
func GetQueryParamListValues(queryVars url.Values, varKey string, maxNumItems int) (items []string, err error) {
	// get query parameters values for the provided key
	values, found := queryVars[varKey]
	if !found {
		return []string{}, nil
	}

	// each value may contain a simple value or a list of values, in a comma-separated format
	for _, value := range values {
		items = append(items, strings.Split(value, ",")...)
		if len(items) > maxNumItems {
			return []string{}, errs.ErrTooManyQueryParameters
		}
	}
	return items, nil
}

// Slice is a utility function to cut a slice according to the provided offset and limit.
func Slice(full []models.Dimension, offset, limit int) (sliced []models.Dimension) {
	end := offset + limit
	if end > len(full) {
		end = len(full)
	}

	if offset > len(full) {
		return []models.Dimension{}
	}
	return full[offset:end]
}

// SliceStr is a utility function to cut a slice of *strings according to the provided offset and limit.
func SliceStr(full []*string, offset, limit int) (sliced []*string) {
	end := offset + limit
	if end > len(full) {
		end = len(full)
	}

	if offset > len(full) {
		return []*string{}
	}
	return full[offset:end]
}

func RewriteDatasetsBasedOnAuth(ctx context.Context, results []*models.DatasetUpdate, authorised bool, linksBuilder *links.Builder) ([]*models.Dataset, error) {
	items := []*models.Dataset{}
	for _, item := range results {
		if item.Current != nil {
			err := RewriteDatasetLinks(ctx, item.Current.Links, linksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'current' links", err)
				return nil, err
			}
			items = append(items, item.Current)
		}

		if authorised && item.Next != nil {
			err := RewriteDatasetLinks(ctx, item.Next.Links, linksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'next' links", err)
				return nil, err
			}
			items = append(items, item.Next)
		}
	}

	return items, nil
}

func RewriteDatasetLinks(ctx context.Context, oldLinks *models.DatasetLinks, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		oldLinks.AccessRights,
		oldLinks.Editions,
		oldLinks.LatestVersion,
		oldLinks.Self,
		oldLinks.Taxonomy,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteDimensions(ctx context.Context, results []models.Dimension, linksBuilder *links.Builder) ([]models.Dimension, error) {
	items := []models.Dimension{}
	for _, item := range results {
		err := RewriteDimensionLinks(ctx, &item.Links, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' links", err)
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func RewriteDimensionLinks(ctx context.Context, oldLinks *models.DimensionLink, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		&oldLinks.CodeList,
		&oldLinks.Options,
		&oldLinks.Version,
	}

	var err error

	for _, link := range prevLinks {
		if link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewritePublicDimensionOptions(ctx context.Context, results []*models.PublicDimensionOption, linksBuilder *links.Builder) ([]*models.PublicDimensionOption, error) {
	items := []*models.PublicDimensionOption{}
	for _, item := range results {
		err := RewriteDimensionOptionLinks(ctx, &item.Links, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite 'current' links", err)
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func RewriteDimensionOptions(ctx context.Context, results []*models.DimensionOption, linksBuilder *links.Builder) error {
	var err error
	for _, item := range results {
		err = RewriteDimensionOptionLinks(ctx, &item.Links, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension option links", err)
			return err
		}
	}
	return nil
}

func RewriteDimensionOptionLinks(ctx context.Context, oldLinks *models.DimensionOptionLinks, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		&oldLinks.Code,
		&oldLinks.CodeList,
		&oldLinks.Version,
	}

	var err error

	for _, link := range prevLinks {
		if link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteEditionsBasedOnAuth(ctx context.Context, results []*models.EditionUpdate, authorised bool, linksBuilder *links.Builder) ([]*models.Edition, error) {
	items := []*models.Edition{}
	for _, item := range results {
		if item.Current != nil {
			err := RewriteEditionLinks(ctx, item.Current.Links, linksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'current' links", err)
				return nil, err
			}
			items = append(items, item.Current)
		}

		if authorised && item.Next != nil {
			err := RewriteEditionLinks(ctx, item.Next.Links, linksBuilder)
			if err != nil {
				log.Error(ctx, "failed to rewrite 'next' links", err)
				return nil, err
			}
			items = append(items, item.Next)
		}
	}

	return items, nil
}

func RewriteEditionLinks(ctx context.Context, oldLinks *models.EditionUpdateLinks, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		oldLinks.Dataset,
		oldLinks.LatestVersion,
		oldLinks.Self,
		oldLinks.Versions,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteMetadataDimensionsLinks(ctx context.Context, results []models.Dimension, linksBuilder *links.Builder) ([]models.Dimension, error) {
	items := []models.Dimension{}
	var err error

	for _, item := range results {
		if item.HRef != "" {
			item.HRef, err = linksBuilder.BuildLink(item.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": item.HRef})
				return nil, err
			}
			items = append(items, item)
		}
	}
	return items, nil
}

func RewriteMetadataLinks(ctx context.Context, oldLinks *models.MetadataLinks, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		oldLinks.AccessRights,
		oldLinks.Self,
		oldLinks.Spatial,
		oldLinks.Version,
		oldLinks.WebsiteVersion,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}

func RewriteVersions(ctx context.Context, results []models.Version, linksBuilder *links.Builder) ([]models.Version, error) {
	items := []models.Version{}

	var err error

	for _, item := range results {
		item.Dimensions, err = RewriteLinkToEachDimension(ctx, item.Dimensions, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension links", err)
			return nil, err
		}

		err = RewriteVersionLinks(ctx, item.Links, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite version links", err)
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

func RewriteLinkToEachDimension(ctx context.Context, results []models.Dimension, linksBuilder *links.Builder) ([]models.Dimension, error) {
	items := []models.Dimension{}

	var err error

	for _, item := range results {
		item.HRef, err = linksBuilder.BuildLink(item.HRef)
		if err != nil {
			log.Error(ctx, "failed to rewrite link", err, log.Data{"link": item.HRef})
			return nil, err
		}

		err = RewriteDimensionLinks(ctx, &item.Links, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension links", err)
			return nil, err
		}

		items = append(items, item)
	}
	return items, nil
}

func RewriteVersionLinks(ctx context.Context, oldLinks *models.VersionLinks, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		oldLinks.Dataset,
		oldLinks.Dimensions,
		oldLinks.Edition,
		oldLinks.Self,
		oldLinks.Spatial,
		oldLinks.Version,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}

	return nil
}

func RewriteInstances(ctx context.Context, results []*models.Instance, linksBuilder *links.Builder) error {
	var err error

	for _, item := range results {
		item.Dimensions, err = RewriteLinkToEachDimension(ctx, item.Dimensions, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite dimension links", err)
			return err
		}

		err = RewriteInstanceLinks(ctx, item.Links, linksBuilder)
		if err != nil {
			log.Error(ctx, "failed to rewrite instance links", err)
			return err
		}
	}
	return nil

}

func RewriteInstanceLinks(ctx context.Context, oldLinks *models.InstanceLinks, linksBuilder *links.Builder) error {
	prevLinks := []*models.LinkObject{
		oldLinks.Dataset,
		oldLinks.Dimensions,
		oldLinks.Edition,
		oldLinks.Job,
		oldLinks.Self,
		oldLinks.Spatial,
		oldLinks.Version,
	}

	var err error

	for _, link := range prevLinks {
		if link != nil && link.HRef != "" {
			link.HRef, err = linksBuilder.BuildLink(link.HRef)
			if err != nil {
				log.Error(ctx, "failed to rewrite link", err, log.Data{"link": link.HRef})
				return err
			}
		}
	}
	return nil
}
