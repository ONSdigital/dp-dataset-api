package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const addVersionDetailsToInstance = "MATCH (i:`_%s_Instance`) SET i.dataset_id = {dataset_id}, i.edition = {edition}, i.version = {version} RETURN i"
const setInstanceIsPublished = "MATCH (i:`_%s_Instance`) SET i.is_published = true"

//go:generate moq -out ./mocks/bolt.go -pkg mocks . DBPool BoltConn BoltStmt BoltResult

type BoltConn bolt.Conn
type BoltStmt bolt.Stmt
type BoltResult bolt.Result

// DBPool provides a pool of database connections
type DBPool interface {
	OpenPool() (bolt.Conn, error)
	Close() error
}

type Neo4j struct {
	Pool DBPool
}

func (c *Neo4j) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	data := log.Data{
		"instance_id": instanceID,
		"dataset_id":  datasetID,
		"edition":     edition,
		"version":     version,
	}

	conn, err := c.Pool.OpenPool()
	if err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: error opening neo4j connection")
	}

	defer conn.Close()

	query := fmt.Sprintf(addVersionDetailsToInstance, instanceID)
	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: error preparing neo update statement")
	}

	defer stmt.Close()

	params := map[string]interface{}{
		"dataset_id": datasetID,
		"edition":    edition,
		"version":    version,
	}
	expectedResult := int64(len(params))
	result, err := stmt.ExecNeo(params)

	if err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: error executing neo4j update statement")
	}

	stats, ok := result.Metadata()["stats"].(map[string]interface{})
	if !ok {
		return errors.Errorf("neoClient AddVersionDetailsToInstance: error getting query result stats")
	}

	propertiesSet, ok := stats["properties-set"]
	if !ok {
		return errors.Errorf("neoClient AddVersionDetailsToInstance: error verifying query results")
	}

	val, ok := propertiesSet.(int64)
	if !ok {
		return errors.Errorf("neoClient AddVersionDetailsToInstance: error verifying query results")
	}

	if val != expectedResult {
		return errors.Errorf("neoClient AddVersionDetailsToInstance: unexpected rows affected expected %d but was %d", expectedResult, val)
	}

	log.InfoCtx(ctx, "neoClient AddVersionDetailsToInstance: update successful", data)
	return nil
}

func (c *Neo4j) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	data := log.Data{
		"instance_id": instanceID,
	}

	log.InfoCtx(ctx, "neoClient SetInstanceIsPublished: attempting to set is_published property on instance node", data)

	conn, err := c.Pool.OpenPool()
	if err != nil {
		return errors.WithMessage(err, "neoClient SetInstanceIsPublished: error opening neo4j connection")
	}

	defer conn.Close()

	query := fmt.Sprintf(setInstanceIsPublished, instanceID)
	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		return errors.WithMessage(err, "neoClient SetInstanceIsPublished: error preparing neo update statement")
	}

	defer stmt.Close()

	result, err := stmt.ExecNeo(nil)

	if err != nil {
		return errors.WithMessage(err, "neoClient SetInstanceIsPublished: error executing neo4j update statement")
	}

	stats, ok := result.Metadata()["stats"].(map[string]interface{})
	if !ok {
		return errors.Errorf("neoClient SetInstanceIsPublished: error getting query result stats")
	}

	propertiesSet, ok := stats["properties-set"]
	if !ok {
		return errors.Errorf("neoClient SetInstanceIsPublished: error verifying query results")
	}

	val, ok := propertiesSet.(int64)
	if !ok {
		return errors.Errorf("neoClient SetInstanceIsPublished: error verifying query results")
	}

	if val != 1 {
		return errors.Errorf("neoClient SetInstanceIsPublished: unexpected rows affected expected %d but was %d", 1, val)
	}

	log.InfoCtx(ctx, "neoClient SetInstanceIsPublished: update successful", data)
	return nil
}
