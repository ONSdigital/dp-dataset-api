package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const addVersionDetailsToInstance = "MATCH (i:`_%s_Instance`) SET i.dataset_id = {dataset_id}, i.edition = {edition}, i.version = {version} RETURN i"

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

	// TODO do we need to do a defensive check first?

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

	result, err := stmt.ExecNeo(map[string]interface{}{
		"dataset_id": datasetID,
		"edition":    edition,
		"version":    version,
	})

	if err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: error executing neo4j update statement")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.WithMessage(err, "neoClient AddVersionDetailsToInstance: error getting update result data")
	}

	if rowsAffected != 1 {
		return errors.WithMessage(err, fmt.Sprintf("neoClient AddVersionDetailsToInstance: unexpected rows affected expected 1 but was %d", rowsAffected))
	}

	log.InfoCtx(ctx, "neoClient AddVersionDetailsToInstance: update successful", data)
	return nil
}
