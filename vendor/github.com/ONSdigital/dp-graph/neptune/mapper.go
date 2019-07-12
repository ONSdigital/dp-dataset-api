package neptune

import (
	"fmt"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-hierarchy-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gedge/graphson"
)

func (n *NeptuneDB) convertVertexToResponse(v graphson.Vertex, instanceID, dimension string) (res *models.Response, err error) {
	logData := log.Data{"fn": "convertVertexToResponse"}

	res = &models.Response{
		ID: v.GetID(),
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
	return
}

func convertVertexToElement(v graphson.Vertex) (res *models.Element, err error) {
	logData := log.Data{"fn": "convertVertexToElement"}

	res = &models.Element{
		ID: v.GetID(),
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
