package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/neo4j/query"
	"github.com/ONSdigital/go-ns/log"
)

func (n *Neo4j) CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	q := fmt.Sprintf(
		query.CreateHierarchyConstraint,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("creating instance hierarchy code constraint", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.CreateInstanceHierarchyConstraints(ctx, attempt+1, instanceID, dimensionName)
	}

	return nil
}

func (n *Neo4j) CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error {
	q := fmt.Sprintf(
		query.CloneHierarchyNodes,
		codeListID,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"code_list_id":   codeListID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("cloning nodes from the generic hierarchy", logData)

	if _, err := n.Exec(q, map[string]interface{}{"code_list": codeListID}); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.CloneNodes(ctx, attempt+1, instanceID, codeListID, dimensionName)
	}

	return nil
}

func (n *Neo4j) CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error) {
	q := fmt.Sprintf(
		query.CountHierarchyNodes,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("counting nodes in the new instance hierarchy", logData)

	return n.Count(q)
}

func (n *Neo4j) CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error {
	q := fmt.Sprintf(
		query.CloneHierarchyRelationships,
		codeListID,
		codeListID,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"code_list_id":   codeListID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("cloning relationships from the generic hierarchy", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.CloneRelationships(ctx, attempt+1, instanceID, codeListID, dimensionName)
	}

	return nil
}

func (n *Neo4j) SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	q := fmt.Sprintf(
		query.SetNumberOfChildren,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("setting number of children property value on the instance hierarchy nodes", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, instanceID, attempt); finalErr != nil {
			return finalErr
		}

		return n.SetNumberOfChildren(ctx, attempt+1, instanceID, dimensionName)
	}

	return nil
}

func (n *Neo4j) SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	q := fmt.Sprintf(
		query.SetHasData,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("setting has data property on the instance hierarchy", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.SetHasData(ctx, attempt+1, instanceID, dimensionName)
	}

	return nil
}

func (n *Neo4j) MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	q := fmt.Sprintf(query.MarkNodesToRemain,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("marking nodes to remain after trimming sparse branches", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.MarkNodesToRemain(ctx, attempt+1, instanceID, dimensionName)
	}

	return nil
}

func (n *Neo4j) RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	q := fmt.Sprintf(query.RemoveNodesNotMarkedToRemain,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("removing nodes not marked to remain after trimming sparse branches", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.RemoveNodesNotMarkedToRemain(ctx, attempt+1, instanceID, dimensionName)
	}

	return nil
}

func (n *Neo4j) RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	q := fmt.Sprintf(query.RemoveRemainMarker,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"query":          q,
	}

	log.Debug("removing the remain property from the nodes that remain", logData)

	if _, err := n.Exec(q, nil); err != nil {
		if finalErr := n.checkAttempts(err, q, attempt); finalErr != nil {
			return finalErr
		}

		return n.RemoveRemainMarker(ctx, attempt+1, instanceID, dimensionName)
	}

	return nil
}
