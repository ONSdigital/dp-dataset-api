package neo4j

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/dp-graph/neo4j/query"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

// CreateInstanceConstraint creates a constraint on observations inserted for this instance.
func (n *Neo4j) CreateInstanceConstraint(ctx context.Context, i *model.Instance) error {
	if err := i.Validate(); err != nil {
		return err
	}

	createStmt := fmt.Sprintf(query.CreateInstanceObservationConstraint, i.InstanceID)

	if _, err := n.Exec(createStmt, nil); err != nil {
		return errors.Wrap(err, "neo4j.Exec returned an error when creating observation constraint")
	}

	log.Info("created observation constraint", log.Data{"instance_id": i.InstanceID, "statement": createStmt})
	return nil
}

// CreateInstance node in a neo4j graph database
func (n *Neo4j) CreateInstance(ctx context.Context, i *model.Instance) error {
	if err := i.Validate(); err != nil {
		return err
	}

	createStmt := fmt.Sprintf(query.CreateInstance, i.InstanceID, strings.Join(i.CSVHeader, ","))

	if _, err := n.Exec(createStmt, nil); err != nil {
		return errors.Wrap(err, "neo4j.Exec returned an error")
	}

	log.Info("create instance success", log.Data{"instance_id": i.InstanceID, "statement": createStmt})
	return nil
}

// AddDimensions list to the specified instance node.
func (n *Neo4j) AddDimensions(ctx context.Context, i *model.Instance) error {
	if err := i.Validate(); err != nil {
		return err
	}

	stmt := fmt.Sprintf(query.AddInstanceDimensions, i.InstanceID)
	params := map[string]interface{}{"dimensions_list": i.Dimensions}

	if _, err := n.Exec(stmt, params); err != nil {
		return errors.Wrap(err, "neo4j.Exec returned an error")
	}

	log.Info("add instance dimensions success", log.Data{
		"statement":   stmt,
		"params":      params,
		"instance_id": i.InstanceID,
		"dimensions":  i.Dimensions,
	})
	return nil
}

// CreateCodeRelationship links an instance to a code for the given dimension option
func (n *Neo4j) CreateCodeRelationship(ctx context.Context, i *model.Instance, codeListID, code string) error {
	if err := i.Validate(); err != nil {
		return err
	}

	if len(code) == 0 {
		return errors.New("code is required but was empty")
	}

	stmt := fmt.Sprintf(query.CreateInstanceToCodeRelationship, i.InstanceID, codeListID)
	params := map[string]interface{}{"code": code}

	logDebug := map[string]interface{}{
		"statement":   stmt,
		"params":      params,
		"instance_id": i.InstanceID,
		"code":        code,
	}

	result, err := n.Exec(stmt, params)
	if err != nil {
		return errors.Wrap(err, "neo4j.Exec returned an error")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "result.RowsAffected() returned an error")
	}

	logDebug["rows_affected"] = rowsAffected
	if rowsAffected != 1 {
		return errors.New("unexpected number of rows affected. expected 1 but was " + strconv.FormatInt(rowsAffected, 10))
	}

	log.Info("create code relationship success", logDebug)
	return nil
}

// InstanceExists returns true if an instance already exists with the provided id.
func (n *Neo4j) InstanceExists(ctx context.Context, i *model.Instance) (bool, error) {
	c, err := n.Count(fmt.Sprintf(query.CountInstance, i.InstanceID))
	if err != nil {
		return false, errors.Wrap(err, "neo4j.Count returned an error")
	}

	return c >= 1, nil
}

// CountInsertedObservations returns the current number of observations relating to a given instance
func (n *Neo4j) CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error) {
	return n.Count(fmt.Sprintf(query.CountObservations, instanceID))
}

// AddVersionDetailsToInstance updated an instance node to contain details of which
// dataset, edition and version the instance will also be known by
func (n *Neo4j) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	data := log.Data{
		"instance_id": instanceID,
		"dataset_id":  datasetID,
		"edition":     edition,
		"version":     version,
	}

	q := fmt.Sprintf(query.AddVersionDetailsToInstance, instanceID)

	params := map[string]interface{}{
		"dataset_id": datasetID,
		"edition":    edition,
		"version":    version,
	}
	expectedResult := int64(len(params))
	result, err := n.Exec(q, params)

	if err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: error executing neo4j update statement")
	}

	if err := checkPropertiesSet(result, expectedResult); err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: invalid results")
	}

	log.InfoCtx(ctx, "neoClient AddVersionDetailsToInstance: update successful", data)
	return nil
}

// SetInstanceIsPublished sets a flag on an instance node to indicate the published state
func (n *Neo4j) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	data := log.Data{
		"instance_id": instanceID,
	}

	log.InfoCtx(ctx, "neoClient SetInstanceIsPublished: attempting to set is_published property on instance node", data)

	q := fmt.Sprintf(query.SetInstanceIsPublished, instanceID)

	result, err := n.Exec(q, nil)
	if err != nil {
		return errors.WithMessage(err, "neoClient SetInstanceIsPublished: error executing neo4j update statement")
	}

	if err := checkPropertiesSet(result, 1); err != nil {
		return errors.WithMessage(err, "neoClient SetInstanceIsPublished: invalid results")
	}

	log.InfoCtx(ctx, "neoClient SetInstanceIsPublished: update successful", data)
	return nil
}

func checkPropertiesSet(result bolt.Result, expected int64) error {
	stats, ok := result.Metadata()["stats"].(map[string]interface{})
	if !ok {
		return errors.Errorf("error getting query result stats")
	}

	propertiesSet, ok := stats["properties-set"]
	if !ok {
		return errors.Errorf("error verifying query results")
	}

	val, ok := propertiesSet.(int64)
	if !ok {
		return errors.Errorf("error verifying query results")
	}

	if val != expected {
		return errors.Errorf("unexpected rows affected expected %d but was %d", expected, val)
	}

	return nil
}
