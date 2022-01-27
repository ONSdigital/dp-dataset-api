// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package storetest

import (
	"context"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"sync"
)

// Ensure, that CantabularMock does implement store.Cantabular.
// If this is not the case, regenerate this file with moq.
var _ store.Cantabular = &CantabularMock{}

// CantabularMock is a mock implementation of store.Cantabular.
//
// 	func TestSomethingThatUsesCantabular(t *testing.T) {
//
// 		// make and configure a mocked store.Cantabular
// 		mockedCantabular := &CantabularMock{
// 			CheckerFunc: func(contextMoqParam context.Context, checkState *healthcheck.CheckState) error {
// 				panic("mock out the Checker method")
// 			},
// 			PopulationTypesFunc: func(ctx context.Context) ([]models.PopulationType, error) {
// 				panic("mock out the PopulationTypes method")
// 			},
// 		}
//
// 		// use mockedCantabular in code that requires store.Cantabular
// 		// and then make assertions.
//
// 	}
type CantabularMock struct {
	// CheckerFunc mocks the Checker method.
	CheckerFunc func(contextMoqParam context.Context, checkState *healthcheck.CheckState) error

	// PopulationTypesFunc mocks the PopulationTypes method.
	PopulationTypesFunc func(ctx context.Context) ([]models.PopulationType, error)

	// calls tracks calls to the methods.
	calls struct {
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// ContextMoqParam is the contextMoqParam argument value.
			ContextMoqParam context.Context
			// CheckState is the checkState argument value.
			CheckState *healthcheck.CheckState
		}
		// PopulationTypes holds details about calls to the PopulationTypes method.
		PopulationTypes []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
	}
	lockChecker         sync.RWMutex
	lockPopulationTypes sync.RWMutex
}

// Checker calls CheckerFunc.
func (mock *CantabularMock) Checker(contextMoqParam context.Context, checkState *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("CantabularMock.CheckerFunc: method is nil but Cantabular.Checker was just called")
	}
	callInfo := struct {
		ContextMoqParam context.Context
		CheckState      *healthcheck.CheckState
	}{
		ContextMoqParam: contextMoqParam,
		CheckState:      checkState,
	}
	mock.lockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	mock.lockChecker.Unlock()
	return mock.CheckerFunc(contextMoqParam, checkState)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedCantabular.CheckerCalls())
func (mock *CantabularMock) CheckerCalls() []struct {
	ContextMoqParam context.Context
	CheckState      *healthcheck.CheckState
} {
	var calls []struct {
		ContextMoqParam context.Context
		CheckState      *healthcheck.CheckState
	}
	mock.lockChecker.RLock()
	calls = mock.calls.Checker
	mock.lockChecker.RUnlock()
	return calls
}

// PopulationTypes calls PopulationTypesFunc.
func (mock *CantabularMock) PopulationTypes(ctx context.Context) ([]models.PopulationType, error) {
	if mock.PopulationTypesFunc == nil {
		panic("CantabularMock.PopulationTypesFunc: method is nil but Cantabular.PopulationTypes was just called")
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
//     len(mockedCantabular.PopulationTypesCalls())
func (mock *CantabularMock) PopulationTypesCalls() []struct {
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
