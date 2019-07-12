package neptune

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-hierarchy-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gedge/graphson"
)

func (n *NeptuneDB) CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return nil
}

func (n *NeptuneDB) CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.CloneHierarchyNodes,
		codeListID,
		instanceID,
		dimensionName,
		codeListID,
	)
	logData := log.Data{"fn": "CloneNodes",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"code_list_id":   codeListID,
		"dimension_name": dimensionName,
	}
	log.Debug("cloning nodes from the generic hierarchy", logData)

	if _, err = n.getVertices(gremStmt); err != nil {
		log.ErrorC("get", err, logData)
		return
	}

	return
}

func (n *NeptuneDB) CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error) {
	gremStmt := fmt.Sprintf(query.CountHierarchyNodes, instanceID, dimensionName)
	logData := log.Data{
		"fn":             "CountNodes",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
	}
	log.Debug("counting nodes in the new instance hierarchy", logData)

	if count, err = n.getNumber(gremStmt); err != nil {
		log.ErrorC("getNumber", err, logData)
		return
	}
	return
}

func (n *NeptuneDB) CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.CloneHierarchyRelationships,
		codeListID,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)
	logData := log.Data{
		"fn":             "CloneRelationships",
		"instance_id":    instanceID,
		"code_list_id":   codeListID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}
	log.Debug("cloning relationships from the generic hierarchy", logData)

	if _, err = n.getEdges(gremStmt); err != nil {
		log.ErrorC("getEdges", err, logData)
		return
	}

	return n.RemoveCloneEdges(ctx, attempt, instanceID, dimensionName)
}

func (n *NeptuneDB) RemoveCloneEdges(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.RemoveCloneMarkers,
		instanceID,
		dimensionName,
	)
	logData := log.Data{
		"fn":             "RemoveCloneEdges",
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}
	log.Debug("removing edges to generic hierarchy", logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.ErrorC("exec", err, logData)
		return
	}
	return
}

func (n *NeptuneDB) SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.SetNumberOfChildren,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"fn":             "SetNumberOfChildren",
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Debug("setting number-of-children property value on the instance hierarchy nodes", logData)

	if _, err = n.getVertices(gremStmt); err != nil {
		log.ErrorC("getV", err, logData)
		return
	}

	return
}

func (n *NeptuneDB) SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.SetHasData,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Debug("setting has-data property on the instance hierarchy", logData)

	if _, err = n.getVertices(gremStmt); err != nil {
		log.ErrorC("getV", err, logData)
		return
	}

	return
}

func (n *NeptuneDB) MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(query.MarkNodesToRemain,
		instanceID,
		dimensionName,
		// instanceID,
		// dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Debug("marking nodes to remain after trimming sparse branches", logData)

	if _, err = n.getVertices(gremStmt); err != nil {
		log.ErrorC("getV", err, logData)
		return
	}

	return
}

func (n *NeptuneDB) RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(query.RemoveNodesNotMarkedToRemain, instanceID, dimensionName)
	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Debug("removing nodes not marked to remain after trimming sparse branches", logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.ErrorC("exec", err, logData)
		return
	}
	return
}

func (n *NeptuneDB) RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(query.RemoveRemainMarker, instanceID, dimensionName)
	logData := log.Data{
		"fn":             "RemoveRemainMarker",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
	}
	log.Debug("removing the remain property from the nodes that remain", logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.ErrorC("exec", err, logData)
		return
	}
	return
}

func (n *NeptuneDB) GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (codelistID string, err error) {
	gremStmt := fmt.Sprintf(query.HierarchyExists, instanceID, dimension)
	logData := log.Data{
		"fn":             "GetHierarchyCodelist",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimension,
	}

	var vertex graphson.Vertex
	if vertex, err = n.getVertex(gremStmt); err != nil {
		log.ErrorC("get", err, logData)
		return
	}
	if codelistID, err = vertex.GetProperty("code_list"); err != nil {
		log.ErrorC("bad prop", err, logData)
		return
	}
	return
}

func (n *NeptuneDB) GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (node *models.Response, err error) {
	gremStmt := fmt.Sprintf(query.GetHierarchyRoot, instanceID, dimension)
	logData := log.Data{
		"fn":             "GetHierarchyRoot",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimension,
	}

	var vertex graphson.Vertex
	if vertex, err = n.getVertex(gremStmt); err != nil {
		log.ErrorC("get", err, logData)
		return
	}
	if node, err = n.convertVertexToResponse(vertex, instanceID, dimension); err != nil {
		log.ErrorC("conv", err, logData)
		return
	}
	return
}

func (n *NeptuneDB) GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (node *models.Response, err error) {
	gremStmt := fmt.Sprintf(query.GetHierarchyElement, instanceID, dimension, code)
	logData := log.Data{
		"fn":             "GetHierarchyElement",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"code_list_id":   code,
		"dimension_name": dimension,
	}

	var vertex graphson.Vertex
	if vertex, err = n.getVertex(gremStmt); err != nil {
		log.ErrorC("get", err, logData)
		return
	}
	if node, err = n.convertVertexToResponse(vertex, instanceID, dimension); err != nil {
		log.ErrorC("conv", err, logData)
		return
	}
	return
}
