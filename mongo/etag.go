package mongo

import (
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/models"

	"go.mongodb.org/mongo-driver/bson"
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

func newETagForVersionUpdate(currentVersion *models.Version, update *models.Version) (eTag string, err error) {
	b, err := bson.Marshal(update)
	if err != nil {
		return "", err
	}
	return currentVersion.Hash(b)
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

func newETagForOptions(currentInstance *models.Instance, upserts []*models.CachedDimensionOption, updates []*models.DimensionOption) (eTag string, err error) {
	extraBytes := []byte{}

	// append upserts option bytes to the hash func
	for _, option := range upserts {
		optionBytes, err := bson.Marshal(option)
		if err != nil {
			return "", err
		}
		extraBytes = append(extraBytes, optionBytes...)
	}

	// append updates option bytes to the hash func
	for _, option := range updates {
		optionBytes, err := bson.Marshal(option)
		if err != nil {
			return "", err
		}
		extraBytes = append(extraBytes, optionBytes...)
	}

	return currentInstance.Hash(extraBytes)
}
