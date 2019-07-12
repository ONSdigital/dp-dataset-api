package neptune

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/dp-observation-importer/models"
)

// ErrEmptyFilter is returned if the provided filter is empty.
var ErrEmptyFilter = errors.New("filter is empty")

func (n *NeptuneDB) StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error) {
	if filter == nil {
		return nil, ErrEmptyFilter
	}

	q := fmt.Sprintf(query.GetInstanceHeader, filter.InstanceID)

	q += buildObservationsQuery(filter)
	q += query.GetObservationSelectRowPart

	if limit != nil {
		q += fmt.Sprintf(query.LimitPart, *limit)
	}

	return n.Pool.OpenCursorCtx(ctx, q, nil, nil)
}

func buildObservationsQuery(f *observation.Filter) string {
	if f.IsEmpty() {
		return fmt.Sprintf(query.GetAllObservationsPart, f.InstanceID)
	}

	q := fmt.Sprintf(query.GetObservationsPart, f.InstanceID)

	for _, dim := range f.DimensionFilters {
		if len(dim.Options) == 0 {
			continue
		}

		for i, opt := range dim.Options {
			dim.Options[i] = fmt.Sprintf("'%s'", opt)
		}

		q += fmt.Sprintf(query.GetObservationDimensionPart, f.InstanceID, dim.Name, strings.Join(dim.Options, ",")) + ","
	}

	//remove trailing comma and close match statement
	q = strings.Trim(q, ",")
	q += ")"

	return q
}

func (n *NeptuneDB) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error {
	return nil
}
