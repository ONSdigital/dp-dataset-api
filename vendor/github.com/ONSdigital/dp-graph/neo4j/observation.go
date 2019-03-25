package neo4j

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/dp-observation-importer/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
)

// StreamCSVRows returns a reader allowing individual CSV rows to be read.
// Rows returned can be limited, to stop this pass in nil. If filter.DimensionFilters
// is nil, empty or contains only empty values then a StreamRowReader for the entire dataset will be returned.
func (n *Neo4j) StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error) {

	headerRowQuery := fmt.Sprintf("MATCH (i:`_%s_Instance`) RETURN i.header as row", filter.InstanceID)

	unionQuery := headerRowQuery + " UNION ALL " + createObservationQuery(filter)

	if limit != nil {
		limitAsString := strconv.Itoa(*limit)
		unionQuery += " LIMIT " + limitAsString
	}

	log.Info("neo4j query", log.Data{
		"filterID":   filter.FilterID,
		"instanceID": filter.InstanceID,
		"query":      unionQuery,
	})

	return n.StreamRows(unionQuery)
}

func createObservationQuery(filter *observation.Filter) string {
	if filter.IsEmpty() {
		// if no dimension filter are specified than match all observations
		log.Info("no dimension filters supplied, generating entire dataset query", log.Data{
			"filterID":   filter.FilterID,
			"instanceID": filter.InstanceID,
		})
		return fmt.Sprintf("MATCH(o: `_%s_observation`) return o.value as row", filter.InstanceID)
	}

	matchDimensions := "MATCH "
	where := " WHERE "

	count := 0
	for _, dimension := range filter.DimensionFilters {
		// If the dimension options is empty then don't bother specifying in the query as this will exclude all matches.
		if len(dimension.Options) > 0 {
			if count > 0 {
				matchDimensions += ", "
				where += " AND "
			}

			matchDimensions += fmt.Sprintf("(o)-[:isValueOf]->(`%s`:`_%s_%s`)", dimension.Name, filter.InstanceID, dimension.Name)
			where += createOptionList(dimension.Name, dimension.Options)
			count++
		}
	}

	return matchDimensions + where + " RETURN o.value AS row"
}

func createOptionList(name string, opts []string) string {
	var q []string

	for _, o := range opts {
		q = append(q, fmt.Sprintf("`%s`.value='%s'", name, o))
	}

	return fmt.Sprintf("(%s)", strings.Join(q, " OR "))
}

// InsertObservationBatch creates a batch query based on a provided list of
// observations and attempts to insert them by bulk to the database
func (n *Neo4j) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error {
	query := buildInsertObservationQuery(instanceID, observations)
	if len(query) == 0 {
		return errors.New("failed to create query for batch")
	}

	queryParameters, err := createParams(observations, dimensionIDs)
	if err != nil {
		return errors.Wrap(err, "failed to create query parameters for batch query")
	}

	queryResult, err := n.Exec(query, queryParameters)
	if err != nil {
		if neoErr := n.checkAttempts(err, instanceID, attempt); neoErr != nil {
			return errors.Wrap(err, "observation batch save failed")
		}

		log.Info("got an error when saving observations, attempting to retry", log.Data{
			"instance_id":  instanceID,
			"retry_number": attempt,
			"max_attempts": n.maxRetries,
		})

		return n.InsertObservationBatch(ctx, attempt+1, instanceID, observations, dimensionIDs)
	}

	rowsAffected, err := queryResult.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "error attempting to get number of rows affected in query result")
	}

	log.Info("successfully saved observation batch", log.Data{"rows_affected": rowsAffected, "instance_id": instanceID})
	return nil
}

// createParams creates parameters to inject into an insert query for each observation.
func createParams(observations []*models.Observation, dimensionIDs map[string]string) (map[string]interface{}, error) {

	rows := make([]interface{}, 0)

	for _, observation := range observations {

		row := map[string]interface{}{
			"v": observation.Row,
			"i": observation.RowIndex,
		}

		for _, option := range observation.DimensionOptions {

			dimensionName := strings.ToLower(option.DimensionName)

			dimensionLookUp := observation.InstanceID + "_" + dimensionName + "_" + option.Name

			nodeID, ok := dimensionIDs[dimensionLookUp]
			if !ok {
				return nil, fmt.Errorf("No nodeId found for %s", dimensionLookUp)
			}

			row[dimensionName] = nodeID
		}

		rows = append(rows, row)
	}

	return map[string]interface{}{"rows": rows}, nil
}

// buildInsertObservationQuery creates an instance specific insert query.
func buildInsertObservationQuery(instanceID string, observations []*models.Observation) string {
	if len(instanceID) == 0 || len(observations) == 0 {
		return ""
	}

	query := "UNWIND $rows AS row"

	match := " MATCH "
	where := " WHERE "
	create := fmt.Sprintf(" CREATE (o:`_%s_observation` { value:row.v, rowIndex:row.i }), ", instanceID)

	index := 0

	for _, option := range observations[0].DimensionOptions {

		if index != 0 {
			match += ", "
			where += " AND "
			create += ", "
		}
		optionName := strings.ToLower(option.DimensionName)

		match += fmt.Sprintf("(`%s`:`_%s_%s`)", optionName, instanceID, optionName)
		where += fmt.Sprintf("id(`%s`) = toInt(row.`%s`)", optionName, optionName)
		create += fmt.Sprintf("(o)-[:isValueOf]->(`%s`)", optionName)
		index++
	}

	query += match + where + create

	return query
}
