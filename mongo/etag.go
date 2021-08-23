package mongo

import (
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/globalsign/mgo/bson"
)

// AnyETag represents the wildchar that corresponds to not check the ETag value for update requests
const AnyETag = "*"

func newETagForUpdate(currentInstance *models.Instance, update *models.Instance) (eTag string, err error) {
	b, err := bson.Marshal(update)
	if err != nil {
		return "", err
	}
	return currentInstance.Hash(b)
}

func newETagForAddEvent(currentInstance *models.Instance, event *models.Event) (eTag string, err error) {
	b, err := bson.Marshal(event)
	if err != nil {
		return "", err
	}
	return currentInstance.Hash(b)
}

func newETagForObservationsInserted(currentInstance *models.Instance, observationInserted int64) (eTag string, err error) {
	b := []byte(fmt.Sprintf("observationInserted%d", observationInserted))
	return currentInstance.Hash(b)
}

func newETagForStateUpdate(currentInstance *models.Instance, state string) (eTag string, err error) {
	b := []byte(fmt.Sprintf("state%s", state))
	return currentInstance.Hash(b)
}

func newETagForHierarchyTaskStateUpdate(currentInstance *models.Instance, dimension, state string) (eTag string, err error) {
	b := []byte(fmt.Sprintf("hierarchyTask_dimension%sstate%s", dimension, state))
	return currentInstance.Hash(b)
}

func newETagForBuildSearchTaskStateUpdate(currentInstance *models.Instance, dimension, state string) (eTag string, err error) {
	b := []byte(fmt.Sprintf("buildSearchTask_dimension%sstate%s", dimension, state))
	return currentInstance.Hash(b)
}

func newETagForNodeIDAndOrder(currentInstance *models.Instance, nodeID string, order *int) (eTag string, err error) {
	b := []byte(nodeID)
	if order != nil {
		b = []byte(fmt.Sprintf("%s%d", nodeID, &order))
	}
	return currentInstance.Hash(b)
}

func newETagForAddDimensionOptions(currentInstance *models.Instance, options []*models.CachedDimensionOption) (eTag string, err error) {
	extraBytes := []byte{}
	for _, option := range options {
		optionBytes, err := bson.Marshal(option)
		if err != nil {
			return "", err
		}
		extraBytes = append(extraBytes, optionBytes...)
	}
	return currentInstance.Hash(extraBytes)
}
