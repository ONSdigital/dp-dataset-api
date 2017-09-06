// Code generated by moq; DO NOT EDIT
// github.com/matryer/moq

package backendtest

import (
	"github.com/ONSdigital/dp-dataset-api/models"
	"sync"
)

var (
	lockBackendMockAddDimensionToInstance        sync.RWMutex
	lockBackendMockAddEventToInstance            sync.RWMutex
	lockBackendMockAddInstance                   sync.RWMutex
	lockBackendMockGetDataset                    sync.RWMutex
	lockBackendMockGetDatasets                   sync.RWMutex
	lockBackendMockGetDimensionNodesFromInstance sync.RWMutex
	lockBackendMockGetEdition                    sync.RWMutex
	lockBackendMockGetEditions                   sync.RWMutex
	lockBackendMockGetInstance                   sync.RWMutex
	lockBackendMockGetInstances                  sync.RWMutex
	lockBackendMockGetUniqueDimensionValues      sync.RWMutex
	lockBackendMockGetVersion                    sync.RWMutex
	lockBackendMockGetVersions                   sync.RWMutex
	lockBackendMockUpdateDimensionNodeID         sync.RWMutex
	lockBackendMockUpdateInstance                sync.RWMutex
	lockBackendMockUpdateObservationInserted     sync.RWMutex
	lockBackendMockUpsertContact                 sync.RWMutex
	lockBackendMockUpsertDataset                 sync.RWMutex
	lockBackendMockUpsertEdition                 sync.RWMutex
	lockBackendMockUpsertVersion                 sync.RWMutex
)

// BackendMock is a mock implementation of Backend.
//
//     func TestSomethingThatUsesBackend(t *testing.T) {
//
//         // make and configure a mocked Backend
//         mockedBackend := &BackendMock{
//             AddDimensionToInstanceFunc: func(dimension *models.DimensionNode) error {
// 	               panic("TODO: mock out the AddDimensionToInstance method")
//             },
//             AddEventToInstanceFunc: func(instanceId string, event *models.Event) error {
// 	               panic("TODO: mock out the AddEventToInstance method")
//             },
//             AddInstanceFunc: func(instance *models.Instance) (*models.Instance, error) {
// 	               panic("TODO: mock out the AddInstance method")
//             },
//             GetDatasetFunc: func(id string) (*models.Dataset, error) {
// 	               panic("TODO: mock out the GetDataset method")
//             },
//             GetDatasetsFunc: func() (*models.DatasetResults, error) {
// 	               panic("TODO: mock out the GetDatasets method")
//             },
//             GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
// 	               panic("TODO: mock out the GetDimensionNodesFromInstance method")
//             },
//             GetEditionFunc: func(datasetID string, editionID string) (*models.Edition, error) {
// 	               panic("TODO: mock out the GetEdition method")
//             },
//             GetEditionsFunc: func(id string) (*models.EditionResults, error) {
// 	               panic("TODO: mock out the GetEditions method")
//             },
//             GetInstanceFunc: func(ID string) (*models.Instance, error) {
// 	               panic("TODO: mock out the GetInstance method")
//             },
//             GetInstancesFunc: func(in1 string) (*models.InstanceResults, error) {
// 	               panic("TODO: mock out the GetInstances method")
//             },
//             GetUniqueDimensionValuesFunc: func(id string, dimension string) (*models.DimensionValues, error) {
// 	               panic("TODO: mock out the GetUniqueDimensionValues method")
//             },
//             GetVersionFunc: func(datasetID string, editionID string, versionID string) (*models.Version, error) {
// 	               panic("TODO: mock out the GetVersion method")
//             },
//             GetVersionsFunc: func(datasetID string, editionID string) (*models.VersionResults, error) {
// 	               panic("TODO: mock out the GetVersions method")
//             },
//             UpdateDimensionNodeIDFunc: func(dimension *models.DimensionNode) error {
// 	               panic("TODO: mock out the UpdateDimensionNodeID method")
//             },
//             UpdateInstanceFunc: func(id string, instance *models.Instance) error {
// 	               panic("TODO: mock out the UpdateInstance method")
//             },
//             UpdateObservationInsertedFunc: func(id string, observationInserted int64) error {
// 	               panic("TODO: mock out the UpdateObservationInserted method")
//             },
//             UpsertContactFunc: func(id string, update interface{}) error {
// 	               panic("TODO: mock out the UpsertContact method")
//             },
//             UpsertDatasetFunc: func(id string, update interface{}) error {
// 	               panic("TODO: mock out the UpsertDataset method")
//             },
//             UpsertEditionFunc: func(id string, update interface{}) error {
// 	               panic("TODO: mock out the UpsertEdition method")
//             },
//             UpsertVersionFunc: func(id string, update interface{}) error {
// 	               panic("TODO: mock out the UpsertVersion method")
//             },
//         }
//
//         // TODO: use mockedBackend in code that requires Backend
//         //       and then make assertions.
//
//     }
type BackendMock struct {
	// AddDimensionToInstanceFunc mocks the AddDimensionToInstance method.
	AddDimensionToInstanceFunc func(dimension *models.DimensionNode) error

	// AddEventToInstanceFunc mocks the AddEventToInstance method.
	AddEventToInstanceFunc func(instanceId string, event *models.Event) error

	// AddInstanceFunc mocks the AddInstance method.
	AddInstanceFunc func(instance *models.Instance) (*models.Instance, error)

	// GetDatasetFunc mocks the GetDataset method.
	GetDatasetFunc func(id string) (*models.Dataset, error)

	// GetDatasetsFunc mocks the GetDatasets method.
	GetDatasetsFunc func() (*models.DatasetResults, error)

	// GetDimensionNodesFromInstanceFunc mocks the GetDimensionNodesFromInstance method.
	GetDimensionNodesFromInstanceFunc func(id string) (*models.DimensionNodeResults, error)

	// GetEditionFunc mocks the GetEdition method.
	GetEditionFunc func(datasetID string, editionID string) (*models.Edition, error)

	// GetEditionsFunc mocks the GetEditions method.
	GetEditionsFunc func(id string) (*models.EditionResults, error)

	// GetInstanceFunc mocks the GetInstance method.
	GetInstanceFunc func(ID string) (*models.Instance, error)

	// GetInstancesFunc mocks the GetInstances method.
	GetInstancesFunc func(in1 string) (*models.InstanceResults, error)

	// GetUniqueDimensionValuesFunc mocks the GetUniqueDimensionValues method.
	GetUniqueDimensionValuesFunc func(id string, dimension string) (*models.DimensionValues, error)

	// GetVersionFunc mocks the GetVersion method.
	GetVersionFunc func(datasetID string, editionID string, versionID string) (*models.Version, error)

	// GetVersionsFunc mocks the GetVersions method.
	GetVersionsFunc func(datasetID string, editionID string) (*models.VersionResults, error)

	// UpdateDimensionNodeIDFunc mocks the UpdateDimensionNodeID method.
	UpdateDimensionNodeIDFunc func(dimension *models.DimensionNode) error

	// UpdateInstanceFunc mocks the UpdateInstance method.
	UpdateInstanceFunc func(id string, instance *models.Instance) error

	// UpdateObservationInsertedFunc mocks the UpdateObservationInserted method.
	UpdateObservationInsertedFunc func(id string, observationInserted int64) error

	// UpsertContactFunc mocks the UpsertContact method.
	UpsertContactFunc func(id string, update interface{}) error

	// UpsertDatasetFunc mocks the UpsertDataset method.
	UpsertDatasetFunc func(id string, update interface{}) error

	// UpsertEditionFunc mocks the UpsertEdition method.
	UpsertEditionFunc func(id string, update interface{}) error

	// UpsertVersionFunc mocks the UpsertVersion method.
	UpsertVersionFunc func(id string, update interface{}) error

	// calls tracks calls to the methods.
	calls struct {
		// AddDimensionToInstance holds details about calls to the AddDimensionToInstance method.
		AddDimensionToInstance []struct {
			// Dimension is the dimension argument value.
			Dimension *models.DimensionNode
		}
		// AddEventToInstance holds details about calls to the AddEventToInstance method.
		AddEventToInstance []struct {
			// InstanceId is the instanceId argument value.
			InstanceId string
			// Event is the event argument value.
			Event *models.Event
		}
		// AddInstance holds details about calls to the AddInstance method.
		AddInstance []struct {
			// Instance is the instance argument value.
			Instance *models.Instance
		}
		// GetDataset holds details about calls to the GetDataset method.
		GetDataset []struct {
			// Id is the id argument value.
			Id string
		}
		// GetDatasets holds details about calls to the GetDatasets method.
		GetDatasets []struct {
		}
		// GetDimensionNodesFromInstance holds details about calls to the GetDimensionNodesFromInstance method.
		GetDimensionNodesFromInstance []struct {
			// Id is the id argument value.
			Id string
		}
		// GetEdition holds details about calls to the GetEdition method.
		GetEdition []struct {
			// DatasetID is the datasetID argument value.
			DatasetID string
			// EditionID is the editionID argument value.
			EditionID string
		}
		// GetEditions holds details about calls to the GetEditions method.
		GetEditions []struct {
			// Id is the id argument value.
			Id string
		}
		// GetInstance holds details about calls to the GetInstance method.
		GetInstance []struct {
			// ID is the ID argument value.
			ID string
		}
		// GetInstances holds details about calls to the GetInstances method.
		GetInstances []struct {
			// In1 is the in1 argument value.
			In1 string
		}
		// GetUniqueDimensionValues holds details about calls to the GetUniqueDimensionValues method.
		GetUniqueDimensionValues []struct {
			// Id is the id argument value.
			Id string
			// Dimension is the dimension argument value.
			Dimension string
		}
		// GetVersion holds details about calls to the GetVersion method.
		GetVersion []struct {
			// DatasetID is the datasetID argument value.
			DatasetID string
			// EditionID is the editionID argument value.
			EditionID string
			// VersionID is the versionID argument value.
			VersionID string
		}
		// GetVersions holds details about calls to the GetVersions method.
		GetVersions []struct {
			// DatasetID is the datasetID argument value.
			DatasetID string
			// EditionID is the editionID argument value.
			EditionID string
		}
		// UpdateDimensionNodeID holds details about calls to the UpdateDimensionNodeID method.
		UpdateDimensionNodeID []struct {
			// Dimension is the dimension argument value.
			Dimension *models.DimensionNode
		}
		// UpdateInstance holds details about calls to the UpdateInstance method.
		UpdateInstance []struct {
			// Id is the id argument value.
			Id string
			// Instance is the instance argument value.
			Instance *models.Instance
		}
		// UpdateObservationInserted holds details about calls to the UpdateObservationInserted method.
		UpdateObservationInserted []struct {
			// Id is the id argument value.
			Id string
			// ObservationInserted is the observationInserted argument value.
			ObservationInserted int64
		}
		// UpsertContact holds details about calls to the UpsertContact method.
		UpsertContact []struct {
			// Id is the id argument value.
			Id string
			// Update is the update argument value.
			Update interface{}
		}
		// UpsertDataset holds details about calls to the UpsertDataset method.
		UpsertDataset []struct {
			// Id is the id argument value.
			Id string
			// Update is the update argument value.
			Update interface{}
		}
		// UpsertEdition holds details about calls to the UpsertEdition method.
		UpsertEdition []struct {
			// Id is the id argument value.
			Id string
			// Update is the update argument value.
			Update interface{}
		}
		// UpsertVersion holds details about calls to the UpsertVersion method.
		UpsertVersion []struct {
			// Id is the id argument value.
			Id string
			// Update is the update argument value.
			Update interface{}
		}
	}
}

// AddDimensionToInstance calls AddDimensionToInstanceFunc.
func (mock *BackendMock) AddDimensionToInstance(dimension *models.DimensionNode) error {
	if mock.AddDimensionToInstanceFunc == nil {
		panic("moq: BackendMock.AddDimensionToInstanceFunc is nil but Backend.AddDimensionToInstance was just called")
	}
	callInfo := struct {
		Dimension *models.DimensionNode
	}{
		Dimension: dimension,
	}
	lockBackendMockAddDimensionToInstance.Lock()
	mock.calls.AddDimensionToInstance = append(mock.calls.AddDimensionToInstance, callInfo)
	lockBackendMockAddDimensionToInstance.Unlock()
	return mock.AddDimensionToInstanceFunc(dimension)
}

// AddDimensionToInstanceCalls gets all the calls that were made to AddDimensionToInstance.
// Check the length with:
//     len(mockedBackend.AddDimensionToInstanceCalls())
func (mock *BackendMock) AddDimensionToInstanceCalls() []struct {
	Dimension *models.DimensionNode
} {
	var calls []struct {
		Dimension *models.DimensionNode
	}
	lockBackendMockAddDimensionToInstance.RLock()
	calls = mock.calls.AddDimensionToInstance
	lockBackendMockAddDimensionToInstance.RUnlock()
	return calls
}

// AddEventToInstance calls AddEventToInstanceFunc.
func (mock *BackendMock) AddEventToInstance(instanceId string, event *models.Event) error {
	if mock.AddEventToInstanceFunc == nil {
		panic("moq: BackendMock.AddEventToInstanceFunc is nil but Backend.AddEventToInstance was just called")
	}
	callInfo := struct {
		InstanceId string
		Event      *models.Event
	}{
		InstanceId: instanceId,
		Event:      event,
	}
	lockBackendMockAddEventToInstance.Lock()
	mock.calls.AddEventToInstance = append(mock.calls.AddEventToInstance, callInfo)
	lockBackendMockAddEventToInstance.Unlock()
	return mock.AddEventToInstanceFunc(instanceId, event)
}

// AddEventToInstanceCalls gets all the calls that were made to AddEventToInstance.
// Check the length with:
//     len(mockedBackend.AddEventToInstanceCalls())
func (mock *BackendMock) AddEventToInstanceCalls() []struct {
	InstanceId string
	Event      *models.Event
} {
	var calls []struct {
		InstanceId string
		Event      *models.Event
	}
	lockBackendMockAddEventToInstance.RLock()
	calls = mock.calls.AddEventToInstance
	lockBackendMockAddEventToInstance.RUnlock()
	return calls
}

// AddInstance calls AddInstanceFunc.
func (mock *BackendMock) AddInstance(instance *models.Instance) (*models.Instance, error) {
	if mock.AddInstanceFunc == nil {
		panic("moq: BackendMock.AddInstanceFunc is nil but Backend.AddInstance was just called")
	}
	callInfo := struct {
		Instance *models.Instance
	}{
		Instance: instance,
	}
	lockBackendMockAddInstance.Lock()
	mock.calls.AddInstance = append(mock.calls.AddInstance, callInfo)
	lockBackendMockAddInstance.Unlock()
	return mock.AddInstanceFunc(instance)
}

// AddInstanceCalls gets all the calls that were made to AddInstance.
// Check the length with:
//     len(mockedBackend.AddInstanceCalls())
func (mock *BackendMock) AddInstanceCalls() []struct {
	Instance *models.Instance
} {
	var calls []struct {
		Instance *models.Instance
	}
	lockBackendMockAddInstance.RLock()
	calls = mock.calls.AddInstance
	lockBackendMockAddInstance.RUnlock()
	return calls
}

// GetDataset calls GetDatasetFunc.
func (mock *BackendMock) GetDataset(id string) (*models.Dataset, error) {
	if mock.GetDatasetFunc == nil {
		panic("moq: BackendMock.GetDatasetFunc is nil but Backend.GetDataset was just called")
	}
	callInfo := struct {
		Id string
	}{
		Id: id,
	}
	lockBackendMockGetDataset.Lock()
	mock.calls.GetDataset = append(mock.calls.GetDataset, callInfo)
	lockBackendMockGetDataset.Unlock()
	return mock.GetDatasetFunc(id)
}

// GetDatasetCalls gets all the calls that were made to GetDataset.
// Check the length with:
//     len(mockedBackend.GetDatasetCalls())
func (mock *BackendMock) GetDatasetCalls() []struct {
	Id string
} {
	var calls []struct {
		Id string
	}
	lockBackendMockGetDataset.RLock()
	calls = mock.calls.GetDataset
	lockBackendMockGetDataset.RUnlock()
	return calls
}

// GetDatasets calls GetDatasetsFunc.
func (mock *BackendMock) GetDatasets() (*models.DatasetResults, error) {
	if mock.GetDatasetsFunc == nil {
		panic("moq: BackendMock.GetDatasetsFunc is nil but Backend.GetDatasets was just called")
	}
	callInfo := struct {
	}{}
	lockBackendMockGetDatasets.Lock()
	mock.calls.GetDatasets = append(mock.calls.GetDatasets, callInfo)
	lockBackendMockGetDatasets.Unlock()
	return mock.GetDatasetsFunc()
}

// GetDatasetsCalls gets all the calls that were made to GetDatasets.
// Check the length with:
//     len(mockedBackend.GetDatasetsCalls())
func (mock *BackendMock) GetDatasetsCalls() []struct {
} {
	var calls []struct {
	}
	lockBackendMockGetDatasets.RLock()
	calls = mock.calls.GetDatasets
	lockBackendMockGetDatasets.RUnlock()
	return calls
}

// GetDimensionNodesFromInstance calls GetDimensionNodesFromInstanceFunc.
func (mock *BackendMock) GetDimensionNodesFromInstance(id string) (*models.DimensionNodeResults, error) {
	if mock.GetDimensionNodesFromInstanceFunc == nil {
		panic("moq: BackendMock.GetDimensionNodesFromInstanceFunc is nil but Backend.GetDimensionNodesFromInstance was just called")
	}
	callInfo := struct {
		Id string
	}{
		Id: id,
	}
	lockBackendMockGetDimensionNodesFromInstance.Lock()
	mock.calls.GetDimensionNodesFromInstance = append(mock.calls.GetDimensionNodesFromInstance, callInfo)
	lockBackendMockGetDimensionNodesFromInstance.Unlock()
	return mock.GetDimensionNodesFromInstanceFunc(id)
}

// GetDimensionNodesFromInstanceCalls gets all the calls that were made to GetDimensionNodesFromInstance.
// Check the length with:
//     len(mockedBackend.GetDimensionNodesFromInstanceCalls())
func (mock *BackendMock) GetDimensionNodesFromInstanceCalls() []struct {
	Id string
} {
	var calls []struct {
		Id string
	}
	lockBackendMockGetDimensionNodesFromInstance.RLock()
	calls = mock.calls.GetDimensionNodesFromInstance
	lockBackendMockGetDimensionNodesFromInstance.RUnlock()
	return calls
}

// GetEdition calls GetEditionFunc.
func (mock *BackendMock) GetEdition(datasetID string, editionID string) (*models.Edition, error) {
	if mock.GetEditionFunc == nil {
		panic("moq: BackendMock.GetEditionFunc is nil but Backend.GetEdition was just called")
	}
	callInfo := struct {
		DatasetID string
		EditionID string
	}{
		DatasetID: datasetID,
		EditionID: editionID,
	}
	lockBackendMockGetEdition.Lock()
	mock.calls.GetEdition = append(mock.calls.GetEdition, callInfo)
	lockBackendMockGetEdition.Unlock()
	return mock.GetEditionFunc(datasetID, editionID)
}

// GetEditionCalls gets all the calls that were made to GetEdition.
// Check the length with:
//     len(mockedBackend.GetEditionCalls())
func (mock *BackendMock) GetEditionCalls() []struct {
	DatasetID string
	EditionID string
} {
	var calls []struct {
		DatasetID string
		EditionID string
	}
	lockBackendMockGetEdition.RLock()
	calls = mock.calls.GetEdition
	lockBackendMockGetEdition.RUnlock()
	return calls
}

// GetEditions calls GetEditionsFunc.
func (mock *BackendMock) GetEditions(id string) (*models.EditionResults, error) {
	if mock.GetEditionsFunc == nil {
		panic("moq: BackendMock.GetEditionsFunc is nil but Backend.GetEditions was just called")
	}
	callInfo := struct {
		Id string
	}{
		Id: id,
	}
	lockBackendMockGetEditions.Lock()
	mock.calls.GetEditions = append(mock.calls.GetEditions, callInfo)
	lockBackendMockGetEditions.Unlock()
	return mock.GetEditionsFunc(id)
}

// GetEditionsCalls gets all the calls that were made to GetEditions.
// Check the length with:
//     len(mockedBackend.GetEditionsCalls())
func (mock *BackendMock) GetEditionsCalls() []struct {
	Id string
} {
	var calls []struct {
		Id string
	}
	lockBackendMockGetEditions.RLock()
	calls = mock.calls.GetEditions
	lockBackendMockGetEditions.RUnlock()
	return calls
}

// GetInstance calls GetInstanceFunc.
func (mock *BackendMock) GetInstance(ID string) (*models.Instance, error) {
	if mock.GetInstanceFunc == nil {
		panic("moq: BackendMock.GetInstanceFunc is nil but Backend.GetInstance was just called")
	}
	callInfo := struct {
		ID string
	}{
		ID: ID,
	}
	lockBackendMockGetInstance.Lock()
	mock.calls.GetInstance = append(mock.calls.GetInstance, callInfo)
	lockBackendMockGetInstance.Unlock()
	return mock.GetInstanceFunc(ID)
}

// GetInstanceCalls gets all the calls that were made to GetInstance.
// Check the length with:
//     len(mockedBackend.GetInstanceCalls())
func (mock *BackendMock) GetInstanceCalls() []struct {
	ID string
} {
	var calls []struct {
		ID string
	}
	lockBackendMockGetInstance.RLock()
	calls = mock.calls.GetInstance
	lockBackendMockGetInstance.RUnlock()
	return calls
}

// GetInstances calls GetInstancesFunc.
func (mock *BackendMock) GetInstances(in1 string) (*models.InstanceResults, error) {
	if mock.GetInstancesFunc == nil {
		panic("moq: BackendMock.GetInstancesFunc is nil but Backend.GetInstances was just called")
	}
	callInfo := struct {
		In1 string
	}{
		In1: in1,
	}
	lockBackendMockGetInstances.Lock()
	mock.calls.GetInstances = append(mock.calls.GetInstances, callInfo)
	lockBackendMockGetInstances.Unlock()
	return mock.GetInstancesFunc(in1)
}

// GetInstancesCalls gets all the calls that were made to GetInstances.
// Check the length with:
//     len(mockedBackend.GetInstancesCalls())
func (mock *BackendMock) GetInstancesCalls() []struct {
	In1 string
} {
	var calls []struct {
		In1 string
	}
	lockBackendMockGetInstances.RLock()
	calls = mock.calls.GetInstances
	lockBackendMockGetInstances.RUnlock()
	return calls
}

// GetUniqueDimensionValues calls GetUniqueDimensionValuesFunc.
func (mock *BackendMock) GetUniqueDimensionValues(id string, dimension string) (*models.DimensionValues, error) {
	if mock.GetUniqueDimensionValuesFunc == nil {
		panic("moq: BackendMock.GetUniqueDimensionValuesFunc is nil but Backend.GetUniqueDimensionValues was just called")
	}
	callInfo := struct {
		Id        string
		Dimension string
	}{
		Id:        id,
		Dimension: dimension,
	}
	lockBackendMockGetUniqueDimensionValues.Lock()
	mock.calls.GetUniqueDimensionValues = append(mock.calls.GetUniqueDimensionValues, callInfo)
	lockBackendMockGetUniqueDimensionValues.Unlock()
	return mock.GetUniqueDimensionValuesFunc(id, dimension)
}

// GetUniqueDimensionValuesCalls gets all the calls that were made to GetUniqueDimensionValues.
// Check the length with:
//     len(mockedBackend.GetUniqueDimensionValuesCalls())
func (mock *BackendMock) GetUniqueDimensionValuesCalls() []struct {
	Id        string
	Dimension string
} {
	var calls []struct {
		Id        string
		Dimension string
	}
	lockBackendMockGetUniqueDimensionValues.RLock()
	calls = mock.calls.GetUniqueDimensionValues
	lockBackendMockGetUniqueDimensionValues.RUnlock()
	return calls
}

// GetVersion calls GetVersionFunc.
func (mock *BackendMock) GetVersion(datasetID string, editionID string, versionID string) (*models.Version, error) {
	if mock.GetVersionFunc == nil {
		panic("moq: BackendMock.GetVersionFunc is nil but Backend.GetVersion was just called")
	}
	callInfo := struct {
		DatasetID string
		EditionID string
		VersionID string
	}{
		DatasetID: datasetID,
		EditionID: editionID,
		VersionID: versionID,
	}
	lockBackendMockGetVersion.Lock()
	mock.calls.GetVersion = append(mock.calls.GetVersion, callInfo)
	lockBackendMockGetVersion.Unlock()
	return mock.GetVersionFunc(datasetID, editionID, versionID)
}

// GetVersionCalls gets all the calls that were made to GetVersion.
// Check the length with:
//     len(mockedBackend.GetVersionCalls())
func (mock *BackendMock) GetVersionCalls() []struct {
	DatasetID string
	EditionID string
	VersionID string
} {
	var calls []struct {
		DatasetID string
		EditionID string
		VersionID string
	}
	lockBackendMockGetVersion.RLock()
	calls = mock.calls.GetVersion
	lockBackendMockGetVersion.RUnlock()
	return calls
}

// GetVersions calls GetVersionsFunc.
func (mock *BackendMock) GetVersions(datasetID string, editionID string) (*models.VersionResults, error) {
	if mock.GetVersionsFunc == nil {
		panic("moq: BackendMock.GetVersionsFunc is nil but Backend.GetVersions was just called")
	}
	callInfo := struct {
		DatasetID string
		EditionID string
	}{
		DatasetID: datasetID,
		EditionID: editionID,
	}
	lockBackendMockGetVersions.Lock()
	mock.calls.GetVersions = append(mock.calls.GetVersions, callInfo)
	lockBackendMockGetVersions.Unlock()
	return mock.GetVersionsFunc(datasetID, editionID)
}

// GetVersionsCalls gets all the calls that were made to GetVersions.
// Check the length with:
//     len(mockedBackend.GetVersionsCalls())
func (mock *BackendMock) GetVersionsCalls() []struct {
	DatasetID string
	EditionID string
} {
	var calls []struct {
		DatasetID string
		EditionID string
	}
	lockBackendMockGetVersions.RLock()
	calls = mock.calls.GetVersions
	lockBackendMockGetVersions.RUnlock()
	return calls
}

// UpdateDimensionNodeID calls UpdateDimensionNodeIDFunc.
func (mock *BackendMock) UpdateDimensionNodeID(dimension *models.DimensionNode) error {
	if mock.UpdateDimensionNodeIDFunc == nil {
		panic("moq: BackendMock.UpdateDimensionNodeIDFunc is nil but Backend.UpdateDimensionNodeID was just called")
	}
	callInfo := struct {
		Dimension *models.DimensionNode
	}{
		Dimension: dimension,
	}
	lockBackendMockUpdateDimensionNodeID.Lock()
	mock.calls.UpdateDimensionNodeID = append(mock.calls.UpdateDimensionNodeID, callInfo)
	lockBackendMockUpdateDimensionNodeID.Unlock()
	return mock.UpdateDimensionNodeIDFunc(dimension)
}

// UpdateDimensionNodeIDCalls gets all the calls that were made to UpdateDimensionNodeID.
// Check the length with:
//     len(mockedBackend.UpdateDimensionNodeIDCalls())
func (mock *BackendMock) UpdateDimensionNodeIDCalls() []struct {
	Dimension *models.DimensionNode
} {
	var calls []struct {
		Dimension *models.DimensionNode
	}
	lockBackendMockUpdateDimensionNodeID.RLock()
	calls = mock.calls.UpdateDimensionNodeID
	lockBackendMockUpdateDimensionNodeID.RUnlock()
	return calls
}

// UpdateInstance calls UpdateInstanceFunc.
func (mock *BackendMock) UpdateInstance(id string, instance *models.Instance) error {
	if mock.UpdateInstanceFunc == nil {
		panic("moq: BackendMock.UpdateInstanceFunc is nil but Backend.UpdateInstance was just called")
	}
	callInfo := struct {
		Id       string
		Instance *models.Instance
	}{
		Id:       id,
		Instance: instance,
	}
	lockBackendMockUpdateInstance.Lock()
	mock.calls.UpdateInstance = append(mock.calls.UpdateInstance, callInfo)
	lockBackendMockUpdateInstance.Unlock()
	return mock.UpdateInstanceFunc(id, instance)
}

// UpdateInstanceCalls gets all the calls that were made to UpdateInstance.
// Check the length with:
//     len(mockedBackend.UpdateInstanceCalls())
func (mock *BackendMock) UpdateInstanceCalls() []struct {
	Id       string
	Instance *models.Instance
} {
	var calls []struct {
		Id       string
		Instance *models.Instance
	}
	lockBackendMockUpdateInstance.RLock()
	calls = mock.calls.UpdateInstance
	lockBackendMockUpdateInstance.RUnlock()
	return calls
}

// UpdateObservationInserted calls UpdateObservationInsertedFunc.
func (mock *BackendMock) UpdateObservationInserted(id string, observationInserted int64) error {
	if mock.UpdateObservationInsertedFunc == nil {
		panic("moq: BackendMock.UpdateObservationInsertedFunc is nil but Backend.UpdateObservationInserted was just called")
	}
	callInfo := struct {
		Id                  string
		ObservationInserted int64
	}{
		Id:                  id,
		ObservationInserted: observationInserted,
	}
	lockBackendMockUpdateObservationInserted.Lock()
	mock.calls.UpdateObservationInserted = append(mock.calls.UpdateObservationInserted, callInfo)
	lockBackendMockUpdateObservationInserted.Unlock()
	return mock.UpdateObservationInsertedFunc(id, observationInserted)
}

// UpdateObservationInsertedCalls gets all the calls that were made to UpdateObservationInserted.
// Check the length with:
//     len(mockedBackend.UpdateObservationInsertedCalls())
func (mock *BackendMock) UpdateObservationInsertedCalls() []struct {
	Id                  string
	ObservationInserted int64
} {
	var calls []struct {
		Id                  string
		ObservationInserted int64
	}
	lockBackendMockUpdateObservationInserted.RLock()
	calls = mock.calls.UpdateObservationInserted
	lockBackendMockUpdateObservationInserted.RUnlock()
	return calls
}

// UpsertContact calls UpsertContactFunc.
func (mock *BackendMock) UpsertContact(id string, update interface{}) error {
	if mock.UpsertContactFunc == nil {
		panic("moq: BackendMock.UpsertContactFunc is nil but Backend.UpsertContact was just called")
	}
	callInfo := struct {
		Id     string
		Update interface{}
	}{
		Id:     id,
		Update: update,
	}
	lockBackendMockUpsertContact.Lock()
	mock.calls.UpsertContact = append(mock.calls.UpsertContact, callInfo)
	lockBackendMockUpsertContact.Unlock()
	return mock.UpsertContactFunc(id, update)
}

// UpsertContactCalls gets all the calls that were made to UpsertContact.
// Check the length with:
//     len(mockedBackend.UpsertContactCalls())
func (mock *BackendMock) UpsertContactCalls() []struct {
	Id     string
	Update interface{}
} {
	var calls []struct {
		Id     string
		Update interface{}
	}
	lockBackendMockUpsertContact.RLock()
	calls = mock.calls.UpsertContact
	lockBackendMockUpsertContact.RUnlock()
	return calls
}

// UpsertDataset calls UpsertDatasetFunc.
func (mock *BackendMock) UpsertDataset(id string, update interface{}) error {
	if mock.UpsertDatasetFunc == nil {
		panic("moq: BackendMock.UpsertDatasetFunc is nil but Backend.UpsertDataset was just called")
	}
	callInfo := struct {
		Id     string
		Update interface{}
	}{
		Id:     id,
		Update: update,
	}
	lockBackendMockUpsertDataset.Lock()
	mock.calls.UpsertDataset = append(mock.calls.UpsertDataset, callInfo)
	lockBackendMockUpsertDataset.Unlock()
	return mock.UpsertDatasetFunc(id, update)
}

// UpsertDatasetCalls gets all the calls that were made to UpsertDataset.
// Check the length with:
//     len(mockedBackend.UpsertDatasetCalls())
func (mock *BackendMock) UpsertDatasetCalls() []struct {
	Id     string
	Update interface{}
} {
	var calls []struct {
		Id     string
		Update interface{}
	}
	lockBackendMockUpsertDataset.RLock()
	calls = mock.calls.UpsertDataset
	lockBackendMockUpsertDataset.RUnlock()
	return calls
}

// UpsertEdition calls UpsertEditionFunc.
func (mock *BackendMock) UpsertEdition(id string, update interface{}) error {
	if mock.UpsertEditionFunc == nil {
		panic("moq: BackendMock.UpsertEditionFunc is nil but Backend.UpsertEdition was just called")
	}
	callInfo := struct {
		Id     string
		Update interface{}
	}{
		Id:     id,
		Update: update,
	}
	lockBackendMockUpsertEdition.Lock()
	mock.calls.UpsertEdition = append(mock.calls.UpsertEdition, callInfo)
	lockBackendMockUpsertEdition.Unlock()
	return mock.UpsertEditionFunc(id, update)
}

// UpsertEditionCalls gets all the calls that were made to UpsertEdition.
// Check the length with:
//     len(mockedBackend.UpsertEditionCalls())
func (mock *BackendMock) UpsertEditionCalls() []struct {
	Id     string
	Update interface{}
} {
	var calls []struct {
		Id     string
		Update interface{}
	}
	lockBackendMockUpsertEdition.RLock()
	calls = mock.calls.UpsertEdition
	lockBackendMockUpsertEdition.RUnlock()
	return calls
}

// UpsertVersion calls UpsertVersionFunc.
func (mock *BackendMock) UpsertVersion(id string, update interface{}) error {
	if mock.UpsertVersionFunc == nil {
		panic("moq: BackendMock.UpsertVersionFunc is nil but Backend.UpsertVersion was just called")
	}
	callInfo := struct {
		Id     string
		Update interface{}
	}{
		Id:     id,
		Update: update,
	}
	lockBackendMockUpsertVersion.Lock()
	mock.calls.UpsertVersion = append(mock.calls.UpsertVersion, callInfo)
	lockBackendMockUpsertVersion.Unlock()
	return mock.UpsertVersionFunc(id, update)
}

// UpsertVersionCalls gets all the calls that were made to UpsertVersion.
// Check the length with:
//     len(mockedBackend.UpsertVersionCalls())
func (mock *BackendMock) UpsertVersionCalls() []struct {
	Id     string
	Update interface{}
} {
	var calls []struct {
		Id     string
		Update interface{}
	}
	lockBackendMockUpsertVersion.RLock()
	calls = mock.calls.UpsertVersion
	lockBackendMockUpsertVersion.RUnlock()
	return calls
}
