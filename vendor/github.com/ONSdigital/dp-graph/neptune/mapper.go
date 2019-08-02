package neptune

/*
This module is dedicated to the needs of the hierarchy API.
*/

import (
	"fmt"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-hierarchy-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/graphson"
)

func (n *NeptuneDB) buildHierarchyNodeFromGraphsonVertex(v graphson.Vertex, instanceID, dimension string, wantBreadcrumbs bool) (res *models.Response, err error) {
	logData := log.Data{"fn": "buildHierarchyNodeFromGraphsonVertex"}

	res = &models.Response{}
	// Note we are using the vertex' *code* property for the response model's
	// ID field - because in the case of a hierarchy node, this is the ID
	// used to format links.
	if res.ID, err = v.GetProperty("code"); err != nil {
		log.ErrorC("bad GetProp code", err, logData)
		return
	}

	if res.Label, err = v.GetLabel(); err != nil {
		log.ErrorC("bad label", err, logData)
		return
	}
	if res.NoOfChildren, err = v.GetPropertyInt64("numberOfChildren"); err != nil {
		log.ErrorC("bad numberOfChildren", err, logData)
		return
	}
	if res.HasData, err = v.GetPropertyBool("hasData"); err != nil {
		log.ErrorC("bad hasData", err, logData)
		return
	}
	// Fetch new data from the database concerned with the node's children.
	if res.NoOfChildren > 0 && instanceID != "" {
		var code string
		if code, err = v.GetProperty("code"); err != nil {
			log.ErrorC("bad GetProp code", err, logData)
			return
		}

		gremStmt := fmt.Sprintf(query.GetChildren, instanceID, dimension, code)
		logData["statement"] = gremStmt

		var childVertices []graphson.Vertex
		if childVertices, err = n.getVertices(gremStmt); err != nil {
			log.ErrorC("get", err, logData)
			return
		}
		if int64(len(childVertices)) != res.NoOfChildren {
			logData["num_children_prop"] = res.NoOfChildren
			logData["num_children_get"] = len(childVertices)
			logData["node_id"] = res.ID
			log.Info("child count mismatch", logData)
		}
		var childElement *models.Element
		for _, child := range childVertices {
			if childElement, err = convertVertexToElement(child); err != nil {
				log.ErrorC("converting child", err, logData)
				return
			}
			res.Children = append(res.Children, childElement)
		}
	}
	// Fetch new data from the database concerned with the node's breadcrumbs.
	if wantBreadcrumbs {
		res.Breadcrumbs, err = n.buildBreadcrumbs(instanceID, dimension, res.ID)
		if err != nil {
			log.ErrorC("building breadcrumbs", err, logData)
		}
	}
	return
}

/*
buildBreadcrumbs launches a new query to the database, to trace the (recursive)
parentage of a hierarcy node. It converts the returned chain of parent
graphson vertices into a chain of models.Element, and returns this list of
elements.
*/
func (n *NeptuneDB) buildBreadcrumbs(instanceID, dimension, code string) ([]*models.Element, error) {
	logData := log.Data{"fn": "buildBreadcrumbs"}
	gremStmt := fmt.Sprintf(query.GetAncestry, instanceID, dimension, code)
	logData["statement"] = gremStmt
	ancestorVertices, err := n.getVertices(gremStmt)
	if err != nil {
		log.ErrorC("getVertices", err, logData)
		return nil, err
	}
	elements := []*models.Element{}
	for _, ancestor := range ancestorVertices {
		element, err := convertVertexToElement(ancestor)
		if err != nil {
			log.ErrorC("convertVertexToElement", err, logData)
			return nil, err
		}
		elements = append(elements, element)
	}
	return elements, nil
}

func convertVertexToElement(v graphson.Vertex) (res *models.Element, err error) {
	logData := log.Data{"fn": "convertVertexToElement"}
	res = &models.Element{}
	// Note we are using the vertex' *code* property for the response model's
	// ID field - because in the case of a hierarchy node, this is the ID
	// used to format links.
	if res.ID, err = v.GetProperty("code"); err != nil {
		log.ErrorC("bad GetProp code", err, logData)
		return
	}

	if res.Label, err = v.GetLabel(); err != nil {
		log.ErrorC("bad label", err, logData)
		return
	}
	if res.NoOfChildren, err = v.GetPropertyInt64("numberOfChildren"); err != nil {
		log.ErrorC("bad numberOfChildren", err, logData)
		return
	}
	if res.HasData, err = v.GetPropertyBool("hasData"); err != nil {
		log.ErrorC("bad hasData", err, logData)
		return
	}
	return
}
