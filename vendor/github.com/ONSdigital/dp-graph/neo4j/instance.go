package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/neo4j/query"
	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

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
