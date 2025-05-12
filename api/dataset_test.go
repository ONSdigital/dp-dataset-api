package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"sync"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/application"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/gorilla/mux"

	"github.com/ONSdigital/dp-dataset-api/config"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host      = "http://localhost:22000"
	authToken = "dataset"
)

var (
	datasetPayload                             = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"filterable"}`
	datasetPayloadWithID                       = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"filterable"}`
	datasetPayloadWithEmptyID                  = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyTitle               = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyDescription         = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyNextRelease         = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyKeywords            = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyTopicsAndTypeStatic = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","topics":[]}`
	datasetPayloadWithEmptyContacts            = `{"contacts":[],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","topics":["theme"]}`
	datasetPayloadWithTypeStatic               = `{"id":"123","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`

	codeListAPIURL     = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL      = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL       = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	websiteURL         = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	urlBuilder         = url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)
	enableURLRewriting = false
	enableStateMachine = false
	mu                 sync.Mutex
)

func getAuthorisationHandlerMock() *mocks.AuthHandlerMock {
	return &mocks.AuthHandlerMock{
		Required: &mocks.PermissionCheckCalls{Calls: 0},
	}
}

// GetAPIWithCMDMocks also used in other tests, so exported
func GetAPIWithCMDMocks(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, datasetPermissions, permissions AuthHandler) *DatasetAPI {
	mu.Lock()
	defer mu.Unlock()
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEndpoints = true
	cfg.DefaultLimit = 0
	cfg.DefaultOffset = 0

	mockedMapGeneratedDownloads := map[models.DatasetType]DownloadsGenerator{
		models.Filterable:              mockedGeneratedDownloads,
		models.CantabularFlexibleTable: mockedGeneratedDownloads,
	}

	mockedMapSMGeneratedDownloads := map[models.DatasetType]application.DownloadsGenerator{
		models.Filterable:              mockedGeneratedDownloads,
		models.CantabularBlob:          mockedGeneratedDownloads,
		models.CantabularTable:         mockedGeneratedDownloads,
		models.CantabularFlexibleTable: mockedGeneratedDownloads,
	}

	states := []application.State{application.Published, application.EditionConfirmed, application.Associated}
	transitions := []application.Transition{{
		Label:               "published",
		TargetState:         application.Published,
		AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
		Type:                "v4",
	}, {
		Label:               "associated",
		TargetState:         application.Associated,
		AllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                "v4",
	},
		{
			Label:               "edition-confirmed",
			TargetState:         application.EditionConfirmed,
			AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                "v4",
		},
		{
			Label:               "published",
			TargetState:         application.Published,
			AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
			Type:                "cantabular_flexible_table",
		}, {
			Label:               "associated",
			TargetState:         application.Associated,
			AllowedSourceStates: []string{"edition-confirmed", "associated"},
			Type:                "cantabular_flexible_table",
		},
		{
			Label:               "edition-confirmed",
			TargetState:         application.EditionConfirmed,
			AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                "cantabular_flexible_table",
		},
		{
			Label:               "published",
			TargetState:         application.Published,
			AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
			Type:                "filterable",
		}, {
			Label:               "associated",
			TargetState:         application.Associated,
			AllowedSourceStates: []string{"edition-confirmed", "associated"},
			Type:                "filterable",
		},
		{
			Label:               "edition-confirmed",
			TargetState:         application.EditionConfirmed,
			AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                "filterable",
		},
		{
			Label:               "associated",
			TargetState:         application.Associated,
			AllowedSourceStates: []string{"created"},
			Type:                "static",
		},
		{
			Label:               "published",
			TargetState:         application.Published,
			AllowedSourceStates: []string{"created", "associated", "published"},
			Type:                "static",
		},
		{
			Label:               "associated",
			TargetState:         application.Associated,
			AllowedSourceStates: []string{"created", "associated"},
			Type:                "static",
		}}

	mockStatemachineDatasetAPI := application.StateMachineDatasetAPI{
		DataStore:          store.DataStore{Backend: mockedDataStore},
		DownloadGenerators: mockedMapSMGeneratedDownloads,
		StateMachine:       application.NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore}),
	}

	return Setup(testContext, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedMapGeneratedDownloads, datasetPermissions, permissions, enableURLRewriting, &mockStatemachineDatasetAPI, enableStateMachine)
}

// GetAPIWithCMDMocks also used in other tests, so exported
func GetAPIWithCantabularMocks(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, datasetPermissions, permissions AuthHandler) *DatasetAPI {
	mu.Lock()
	defer mu.Unlock()
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEndpoints = true
	cfg.DefaultLimit = 0
	cfg.DefaultOffset = 0

	mockedMapGeneratedDownloads := map[models.DatasetType]DownloadsGenerator{
		models.CantabularBlob:          mockedGeneratedDownloads,
		models.CantabularFlexibleTable: mockedGeneratedDownloads,
	}

	mockedMapSMGeneratedDownloads := map[models.DatasetType]application.DownloadsGenerator{
		models.Filterable:              mockedGeneratedDownloads,
		models.CantabularBlob:          mockedGeneratedDownloads,
		models.CantabularFlexibleTable: mockedGeneratedDownloads,
	}

	states := []application.State{application.Published, application.EditionConfirmed, application.Associated}
	transitions := []application.Transition{{
		Label:               "published",
		TargetState:         application.Published,
		AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
		Type:                "cantabular_flexible_table",
	}, {
		Label:               "associated",
		TargetState:         application.Associated,
		AllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                "cantabular_flexible_table",
	}, {
		Label:               "edition-confirmed",
		TargetState:         application.EditionConfirmed,
		AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
		Type:                "cantabular_flexible_table",
	},
		{
			Label:               "associated",
			TargetState:         application.Associated,
			AllowedSourceStates: []string{"created"},
			Type:                "static",
		}}

	mockStatemachineDatasetAPI := application.StateMachineDatasetAPI{
		DataStore:          store.DataStore{Backend: mockedDataStore},
		DownloadGenerators: mockedMapSMGeneratedDownloads,
		StateMachine:       application.NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore}),
	}

	return Setup(testContext, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedMapGeneratedDownloads, datasetPermissions, permissions, enableURLRewriting, &mockStatemachineDatasetAPI, enableStateMachine)
}

func createRequestWithAuth(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r
}

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()

	Convey("A successful request to get dataset returns 200 OK response, and limit and offset are delegated to the datastore", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(context.Context, int, int, bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{}, 15, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)

		So(actualResponse, ShouldResemble, []*models.Dataset{})
		So(actualTotalCount, ShouldEqual, 15)
		So(err, ShouldEqual, nil)
		So(mockedDataStore.GetDatasetsCalls()[0].Limit, ShouldEqual, 11)
		So(mockedDataStore.GetDatasetsCalls()[0].Offset, ShouldEqual, 12)
	})

	Convey("A successful request to get datasetwith type query parameter returns 200 OK response, and limit and offset are delegated to the datastore", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?type=static")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{{ID: "123-456", Current: &models.Dataset{ID: "123-456", Type: "static"}, Next: &models.Dataset{ID: "123-456", Type: "static"}}}, 1, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)

		So(actualResponse, ShouldResemble, []*models.Dataset{{ID: "123-456", Type: "static"}})
		So(actualTotalCount, ShouldEqual, 1)
		So(err, ShouldEqual, nil)
		So(mockedDataStore.GetDatasetsByQueryParamsCalls()[0].Limit, ShouldEqual, 11)
		So(mockedDataStore.GetDatasetsByQueryParamsCalls()[0].Offset, ShouldEqual, 12)
	})

	Convey("A successful request to get dataset with is_based_on returns 200 OK response, and limit and offset are delegated to the datastore", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?is_based_on=Example")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{{ID: "123-456", Current: &models.Dataset{ID: "123-456", Type: "static", IsBasedOn: &models.IsBasedOn{ID: "Example"}}, Next: &models.Dataset{ID: "123-456", Type: "static", IsBasedOn: &models.IsBasedOn{ID: "Example"}}}}, 1, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)
		So(actualResponse, ShouldResemble, []*models.Dataset{{ID: "123-456", Type: "static", IsBasedOn: &models.IsBasedOn{ID: "Example"}}})
		So(actualTotalCount, ShouldEqual, 1)
		So(err, ShouldEqual, nil)
		So(mockedDataStore.GetDatasetsByQueryParamsCalls()[0].Limit, ShouldEqual, 11)
		So(mockedDataStore.GetDatasetsByQueryParamsCalls()[0].Offset, ShouldEqual, 12)
	})
}

func TestGetDatasetsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(context.Context, int, int, bool) ([]*models.DatasetUpdate, int, error) {
				return nil, 0, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(actualResponse, ShouldResemble, nil)
		So(actualTotalCount, ShouldEqual, 0)
		So(err, ShouldEqual, errs.ErrInternalServer)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")
	})

	Convey("When the type query is empty return an invalid query parameter error ", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?type=")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(context.Context, int, int, bool) ([]*models.DatasetUpdate, int, error) {
				return nil, 0, errs.ErrInvalidQueryParameter
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 0)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(actualResponse, ShouldResemble, nil)
		So(actualTotalCount, ShouldEqual, 0)
		So(err, ShouldEqual, errs.ErrInvalidQueryParameter)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "invalid query parameter\n")
	})

	Convey("When the is_based_on query is empty return an invalid query parameter error ", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?is_based_on=")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 0)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(actualResponse, ShouldResemble, nil)
		So(actualTotalCount, ShouldEqual, 0)
		So(err, ShouldEqual, errs.ErrInvalidQueryParameter)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "invalid query parameter\n")
	})

	Convey("When the type query contains incorrect dataset type return an dataset type invalid error", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?type=wrongdstype")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return nil, 0, errs.ErrDatasetTypeInvalid
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 1)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(actualResponse, ShouldResemble, nil)
		So(actualTotalCount, ShouldEqual, 0)
		So(err, ShouldEqual, errs.ErrDatasetTypeInvalid)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "invalid dataset type\n")
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("When dataset document has a current sub document return status 200", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document and request is authorised return status 200", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123-456", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When there is no dataset document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("Request with empty dataset ID returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("Request with empty dataset title returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyTitle
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("Request with empty dataset description returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyDescription
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("Request with empty dataset next release returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyNextRelease
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("Request with empty dataset keywords returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyKeywords
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("Request with empty topics returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyTopicsAndTypeStatic
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("Request with empty dataset contacts returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyContacts
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPostDatasetsReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("A successful request to post dataset returns 201 OK response", t, func() {
		b := datasetPayload
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with an empty QMI url returns 201 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with a valid QMI url (path in appropriate url format) returns 201 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http://domain.com/path", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with a valid QMI url (relative path) returns 201 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "/path", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with a valid QMI url (valid host but an empty path) returns 201 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http://domain.com/", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with a valid QMI url (only a valid domain) returns 201 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "domain.com", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("A successful request to post a dataset returns 201 Created response", t, func() {
		b := datasetPayloadWithID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPostDatasetsReturnsError(t *testing.T) {
	Convey("When the request contains duplicate dataset title a forbidden request status is returned", t, func() {
		b := datasetPayloadWithID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return true, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrAddDatasetTitleAlreadyExists.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When CheckDatasetTitleExistFunc return an error, an internal server error status is returned", t, func() {
		b := datasetPayloadWithID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, errors.New("internal error")
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPostDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		b := "{"
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return errs.ErrAddUpdateDatasetBadRequest
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		b := datasetPayload
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request does not contain a valid internal token returns 401", t, func() {
		b := datasetPayload
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the dataset already exists and a request is sent to create the same dataset return status forbidden", t, func() {
		b := datasetPayload
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Next:    &models.Dataset{},
					Current: &models.Dataset{},
				}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "forbidden - dataset already exists\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with invalid QMI url (invalid character) returns bad request", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": ":not a link", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Body.String(), ShouldResemble, "invalid fields: [QMI]\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 0)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with invalid QMI url (scheme only) returns bad request", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http://", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Body.String(), ShouldResemble, "invalid fields: [QMI]\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 0)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When creating the dataset with invalid QMI url (scheme and path only) returns bad request", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http:///path", "title": "test"}}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Body.String(), ShouldResemble, "invalid fields: [QMI]\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 0)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request has an invalid datatype it should return invalid type errorq", t, func() {
		b := `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"nomis_filterable"}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "invalid dataset type\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request body has an empty type field it should create a dataset with type defaulted to filterable", t, func() {
		b := `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":""}`
		// split up expected result since last_updated is added by the API and exact value cannot be known in advance
		res1 := `{"id":"123123","next":{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","id":"123123",`
		res2 := `"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"},"editions":{"href":"http://localhost:22000/datasets/123123/editions"},"self":{"href":"http://localhost:22000/datasets/123123"}},"next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department"},"state":"created","theme":"population","title":"CensusEthnicity","type":"filterable"}}`
		resLastUpdated := `"last_updated":"`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(w.Body.String(), ShouldContainSubstring, res1)
		So(w.Body.String(), ShouldContainSubstring, res2)
		So(w.Body.String(), ShouldContainSubstring, resLastUpdated)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPutDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("A successful request to put dataset returns 200 OK response", t, func() {
		b := datasetPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})

		Convey("and the response body contains the dataset we sent with the request", func() {
			var expected, actual models.Dataset

			err := json.Unmarshal([]byte(datasetPayload), &expected)
			So(err, ShouldBeNil)

			err = json.Unmarshal(w.Body.Bytes(), &actual)
			So(err, ShouldBeNil)

			So(actual, ShouldResemble, expected)
		})
	})

	Convey("A successful request to put dataset with static dataset type returns 200 OK response", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "static"}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})

		Convey("and the response body contains the dataset we sent with the request", func() {
			var expected, actual models.Dataset

			err := json.Unmarshal([]byte(datasetPayloadWithTypeStatic), &expected)
			So(err, ShouldBeNil)

			err = json.Unmarshal(w.Body.Bytes(), &actual)
			So(err, ShouldBeNil)

			So(actual, ShouldResemble, expected)
		})
	})

	Convey("When update dataset type has a value of filterable and stored dataset type is nomis return status ok", t, func() {
		// Dataset type field cannot be updated and hence is ignored in any updates to the dataset

		b := `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"filterable"}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "nomis"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with an empty QMI url returns 200 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with a valid QMI url (path in appropriate url format) returns 200 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http://domain.com/path", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with a valid QMI url (relative path) returns 200 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "/path", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with a valid QMI url (valid host but an empty path) returns 200 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http://domain.com/", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with a valid QMI url (only a valid domain) returns 200 success", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "domain.com", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPutDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		b := "{"
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrAddUpdateDatasetBadRequest
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the dataset document cannot be found return status not found ", t, func() {
		b := datasetPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with invalid QMI url (invalid character) returns bad request", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": ":not a link", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "invalid fields: [QMI]\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with invalid QMI url (scheme only) returns bad request", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http://", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "invalid fields: [QMI]\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset with invalid QMI url (scheme and path only) returns bad request", t, func() {
		b := `{"contacts": [{"email": "testing@hotmail.com", "name": "John Cox", "telephone": "01623 456789"}], "description": "census", "links": {"access_rights": {"href": "http://ons.gov.uk/accessrights"}}, "title": "CensusEthnicity", "theme": "population", "state": "completed", "next_release": "2016-04-04", "publisher": {"name": "The office of national statistics", "type": "government department", "url": "https://www.ons.gov.uk/"}, "type": "filterable", "qmi": {"href": "http:///path", "title": "test"}}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "invalid fields: [QMI]\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request is not authorised to update dataset return status unauthorised", t, func() {
		b := "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the static dataset with a title that already exists returns 403 forbidden", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "static"}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return true, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldResemble, "forbidden - dataset title already exists\n")
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the CheckDatasetTitleExist call fails return an internal server error", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "static"}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestDeleteDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("A successful request to delete dataset returns 200 OK response", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return []*models.EditionUpdate{}, 0, nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})

	Convey("A successful request to delete dataset with editions returns 200 OK response", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				var items []*models.EditionUpdate
				items = append(items, &models.EditionUpdate{})
				return items, 0, nil
			},
			DeleteEditionFunc: func(context.Context, string) error {
				return nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})
}

func TestDeleteDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When a request to delete a published dataset return status forbidden", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return []*models.EditionUpdate{}, 0, nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return []*models.EditionUpdate{}, 0, nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When the dataset document cannot be found return status not found ", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return []*models.EditionUpdate{}, 0, nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document cannot be queried return status 500 ", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errors.New("database is broken")
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return []*models.EditionUpdate{}, 0, nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		b := "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})
}
