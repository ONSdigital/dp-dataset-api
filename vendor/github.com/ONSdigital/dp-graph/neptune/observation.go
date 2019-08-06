package neptune

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/dp-observation-importer/models"
	"github.com/ONSdigital/go-ns/log"
)

// ErrInvalidFilter is returned if the provided filter is nil.
var ErrInvalidFilter = errors.New("nil filter cannot be processed")

// TODO: this global state is only used for metrics in InsertObservationBatch,
// not used in actual code flow, but should be revisited before production use
var batchCount = 0
var totalTime time.Time

func (n *NeptuneDB) StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error) {
	if filter == nil {
		return nil, ErrInvalidFilter
	}

	q := fmt.Sprintf(query.GetInstanceHeaderPart, filter.InstanceID)

	q += buildObservationsQuery(filter)
	q += query.GetObservationSelectRowPart

	if limit != nil {
		q += fmt.Sprintf(query.LimitPart, *limit)
	}

	return n.Pool.OpenStreamCursor(ctx, q, nil, nil)
}

func buildObservationsQuery(f *observation.Filter) string {
	if f.IsEmpty() {
		return fmt.Sprintf(query.GetAllObservationsPart, f.InstanceID)
	}

	q := fmt.Sprintf(query.GetObservationsPart, f.InstanceID)
	var selectOpts []string

	for _, dim := range f.DimensionFilters {
		if len(dim.Options) == 0 {
			continue
		}

		for i, opt := range dim.Options {
			dim.Options[i] = fmt.Sprintf("'%s'", opt)
		}

		selectOpts = append(selectOpts, fmt.Sprintf(query.GetObservationDimensionPart, f.InstanceID, dim.Name, strings.Join(dim.Options, ",")))
	}

	//comma separate dimension option selections and close match statement
	q += strings.Join(selectOpts, ",")
	q += ")"

	return q
}

func (n *NeptuneDB) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionNodeIDs map[string]string) error {
	if len(observations) == 0 {
		log.Info("no observations in batch", log.Data{"instance_ID": instanceID})
		return nil
	}

	bID := batchCount
	batchCount++
	batchStart := time.Now()
	if totalTime.IsZero() {
		totalTime = batchStart
	} else {
		log.Info("opening batch", log.Data{"size": len(observations), "batchID": bID})
	}

	var create string
	for _, o := range observations {
		create += fmt.Sprintf(query.DropObservationRelationships, instanceID, o.Row)
		create += fmt.Sprintf(query.DropObservation, instanceID, o.Row)
		create += fmt.Sprintf(query.CreateObservationPart, instanceID, o.Row, o.RowIndex)
		for _, d := range o.DimensionOptions {
			dimensionName := strings.ToLower(d.DimensionName)
			dimensionLookup := instanceID + "_" + dimensionName + "_" + d.Name

			nodeID, ok := dimensionNodeIDs[dimensionLookup]
			if !ok {
				return fmt.Errorf("no nodeID [%s] found in dimension map", dimensionLookup)
			}

			create += fmt.Sprintf(query.AddObservationRelationshipPart, nodeID, instanceID, d.DimensionName, d.Name)
		}

		create = strings.TrimSuffix(create, ".outV()")
		create += ".iterate() "
	}

	if _, err := n.exec(create); err != nil {
		return err
	}

	log.Info("batch complete", log.Data{"batchID": bID, "elapsed": time.Since(totalTime), "batchTime": time.Since(batchStart)})
	return nil
}
