// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-dataset-api/service"
	"github.com/ONSdigital/dp-dataset-api/store"
	"sync"
)

// Ensure, that CantabularClientMock does implement service.CantabularClient.
// If this is not the case, regenerate this file with moq.
var _ service.CantabularClient = &CantabularClientMock{}

// CantabularClientMock is a mock implementation of service.CantabularClient.
//
// 	func TestSomethingThatUsesCantabularClient(t *testing.T) {
//
// 		// make and configure a mocked service.CantabularClient
// 		mockedCantabularClient := &CantabularClientMock{
// 			PopulationTypesFunc: func(ctx context.Context) []store.CantabularBlob {
// 				panic("mock out the PopulationTypes method")
// 			},
// 		}
//
// 		// use mockedCantabularClient in code that requires service.CantabularClient
// 		// and then make assertions.
//
// 	}
type CantabularClientMock struct {
	// PopulationTypesFunc mocks the PopulationTypes method.
	PopulationTypesFunc func(ctx context.Context) []store.CantabularBlob

	// calls tracks calls to the methods.
	calls struct {
		// PopulationTypes holds details about calls to the PopulationTypes method.
		PopulationTypes []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
	}
	lockPopulationTypes sync.RWMutex
}

// PopulationTypes calls PopulationTypesFunc.
func (mock *CantabularClientMock) PopulationTypes(ctx context.Context) []store.CantabularBlob {
	if mock.PopulationTypesFunc == nil {
		panic("CantabularClientMock.PopulationTypesFunc: method is nil but CantabularClient.PopulationTypes was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	mock.lockPopulationTypes.Lock()
	mock.calls.PopulationTypes = append(mock.calls.PopulationTypes, callInfo)
	mock.lockPopulationTypes.Unlock()
	return mock.PopulationTypesFunc(ctx)
}

// PopulationTypesCalls gets all the calls that were made to PopulationTypes.
// Check the length with:
//     len(mockedCantabularClient.PopulationTypesCalls())
func (mock *CantabularClientMock) PopulationTypesCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	mock.lockPopulationTypes.RLock()
	calls = mock.calls.PopulationTypes
	mock.lockPopulationTypes.RUnlock()
	return calls
}
