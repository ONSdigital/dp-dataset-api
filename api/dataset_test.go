package api

import (
	"bytes"
	"context"
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
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/gorilla/mux"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/smartystreets/goconvey/convey"
)

const (
	host      = "http://localhost:22000"
	authToken = "dataset"
)

var (
	datasetPayload                             = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"filterable"}`
	datasetPayloadWithID                       = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"filterable"}`
	datasetPayloadWithEmptyID                  = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyTitle               = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyDescription         = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyNextRelease         = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyKeywords            = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static"}`
	datasetPayloadWithEmptyThemesAndTypeStatic = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static","themes":[]}`
	datasetPayloadWithEmptyContacts            = `{"contacts":[],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"static","themes":["theme"]}`

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
	convey.So(err, convey.ShouldBeNil)
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
		Label:                "published",
		TargetState:          application.Published,
		AlllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
		Type:                 "v4",
	}, {
		Label:                "associated",
		TargetState:          application.Associated,
		AlllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                 "v4",
	},
		{
			Label:                "edition-confirmed",
			TargetState:          application.EditionConfirmed,
			AlllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                 "v4",
		},
		{
			Label:                "published",
			TargetState:          application.Published,
			AlllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
			Type:                 "cantabular_flexible_table",
		}, {
			Label:                "associated",
			TargetState:          application.Associated,
			AlllowedSourceStates: []string{"edition-confirmed", "associated"},
			Type:                 "cantabular_flexible_table",
		},
		{
			Label:                "edition-confirmed",
			TargetState:          application.EditionConfirmed,
			AlllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                 "cantabular_flexible_table",
		},
		{
			Label:                "published",
			TargetState:          application.Published,
			AlllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
			Type:                 "filterable",
		}, {
			Label:                "associated",
			TargetState:          application.Associated,
			AlllowedSourceStates: []string{"edition-confirmed", "associated"},
			Type:                 "filterable",
		},
		{
			Label:                "edition-confirmed",
			TargetState:          application.EditionConfirmed,
			AlllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                 "filterable",
		},
		{
			Label:                "associated",
			TargetState:          application.Associated,
			AlllowedSourceStates: []string{"created"},
			Type:                 "static",
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
	convey.So(err, convey.ShouldBeNil)
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
		Label:                "published",
		TargetState:          application.Published,
		AlllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
		Type:                 "cantabular_flexible_table",
	}, {
		Label:                "associated",
		TargetState:          application.Associated,
		AlllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                 "cantabular_flexible_table",
	}, {
		Label:                "edition-confirmed",
		TargetState:          application.EditionConfirmed,
		AlllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
		Type:                 "cantabular_flexible_table",
	},
		{
			Label:                "associated",
			TargetState:          application.Associated,
			AlllowedSourceStates: []string{"created"},
			Type:                 "static",
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

	convey.Convey("A successful request to get dataset returns 200 OK response, and limit and offset are delegated to the datastore", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets")
		convey.So(err, convey.ShouldBeNil)
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

		convey.So(actualResponse, convey.ShouldResemble, []*models.Dataset{})
		convey.So(actualTotalCount, convey.ShouldEqual, 15)
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(mockedDataStore.GetDatasetsCalls()[0].Limit, convey.ShouldEqual, 11)
		convey.So(mockedDataStore.GetDatasetsCalls()[0].Offset, convey.ShouldEqual, 12)
	})
}

func TestGetDatasetsReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets")
		convey.So(err, convey.ShouldBeNil)
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
		convey.So(len(mockedDataStore.GetDatasetsCalls()), convey.ShouldEqual, 1)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(actualResponse, convey.ShouldResemble, nil)
		convey.So(actualTotalCount, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldEqual, errs.ErrInternalServer)
		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldEqual, "internal error\n")
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("When dataset document has a current sub document return status 200", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When dataset document has only a next sub document and request is authorised return status 200", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When dataset document has only a next sub document return status 404", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When there is no dataset document return status 404", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("Request with empty dataset ID returns 400 Bad Request", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("Request with empty dataset title returns 400 Bad Request", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("Request with empty dataset description returns 400 Bad Request", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("Request with empty dataset next release returns 400 Bad Request", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("Request with empty dataset keywords returns 400 Bad Request", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("Request with empty themes returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyThemesAndTypeStatic
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("Request with empty dataset contacts returns 400 Bad Request", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	})
}

func TestPostDatasetsReturnsCreated(t *testing.T) {
	t.Parallel()
	convey.Convey("A successful request to post dataset returns 201 OK response", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with an empty QMI url returns 201 success", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with a valid QMI url (path in appropriate url format) returns 201 success", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with a valid QMI url (relative path) returns 201 success", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with a valid QMI url (valid host but an empty path) returns 201 success", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with a valid QMI url (only a valid domain) returns 201 success", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("A successful request to post a dataset returns 201 Created response", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})
}

func TestPostDatasetReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the request contain malformed json a bad request status is returned", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request does not contain a valid internal token returns 401", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldResemble, "unauthenticated request\n")
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the dataset already exists and a request is sent to create the same dataset return status forbidden", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldResemble, "forbidden - dataset already exists\n")
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with invalid QMI url (invalid character) returns bad request", t, func() {
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

		convey.So(w.Body.String(), convey.ShouldResemble, "invalid fields: [QMI]\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(mockedDataStore.UpsertDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with invalid QMI url (scheme only) returns bad request", t, func() {
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

		convey.So(w.Body.String(), convey.ShouldResemble, "invalid fields: [QMI]\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(mockedDataStore.UpsertDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When creating the dataset with invalid QMI url (scheme and path only) returns bad request", t, func() {
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

		convey.So(w.Body.String(), convey.ShouldResemble, "invalid fields: [QMI]\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(mockedDataStore.UpsertDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request has an invalid datatype it should return invalid type errorq", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldResemble, "invalid dataset type\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpsertDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request body has an empty type field it should create a dataset with type defaulted to filterable", t, func() {
		b := `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":""}`
		res := `{"id":"123123","next":{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","id":"123123","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"},"editions":{"href":"http://localhost:22000/datasets/123123/editions"},"self":{"href":"http://localhost:22000/datasets/123123"}},"next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department"},"state":"created","theme":"population","title":"CensusEthnicity","type":"filterable"}}`
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, res)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpsertDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})
}

func TestPutDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	convey.Convey("A successful request to put dataset returns 200 OK response", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When update dataset type has a value of filterable and stored dataset type is nomis return status ok", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with an empty QMI url returns 200 success", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with a valid QMI url (path in appropriate url format) returns 200 success", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with a valid QMI url (relative path) returns 200 success", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with a valid QMI url (valid host but an empty path) returns 200 success", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with a valid QMI url (only a valid domain) returns 200 success", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})
}

func TestPutDatasetReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the request contain malformed json a bad request status is returned", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetCalls()), convey.ShouldEqual, 1)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the dataset document cannot be found return status not found ", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with invalid QMI url (invalid character) returns bad request", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldResemble, "invalid fields: [QMI]\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with invalid QMI url (scheme only) returns bad request", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldResemble, "invalid fields: [QMI]\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When updating the dataset with invalid QMI url (scheme and path only) returns bad request", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldResemble, "invalid fields: [QMI]\n")
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateDatasetCalls(), convey.ShouldHaveLength, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request is not authorised to update dataset return status unauthorised", t, func() {
		b := "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		convey.So(err, convey.ShouldBeNil)
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldResemble, "unauthenticated request\n")

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpdateDatasetCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})
}

func TestDeleteDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	convey.Convey("A successful request to delete dataset returns 200 OK response", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNoContent)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.DeleteDatasetCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("A successful request to delete dataset with editions returns 200 OK response", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNoContent)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.DeleteEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.DeleteDatasetCalls()), convey.ShouldEqual, 1)
	})
}

func TestDeleteDatasetReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When a request to delete a published dataset return status forbidden", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.DeleteEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.DeleteDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.DeleteEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.DeleteDatasetCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When the dataset document cannot be found return status not found ", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNoContent)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.DeleteEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpdateDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset document cannot be queried return status 500 ", t, func() {
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpdateDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		b := "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldResemble, "unauthenticated request\n")
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.DeleteEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.DeleteDatasetCalls()), convey.ShouldEqual, 0)
	})
}

func TestAddDatasetVersionCondensed(t *testing.T) {
	t.Parallel()
	convey.Convey("When dataset and edition exist and instance is added successfully", t, func() {
		b := `{"title":"test-dataset","description":"test dataset","type":"static","next_release":"2025-02-15","alerts":[{"date":"2025-01-15","description":"Correction to the CPIH index for December 2024 due to an error in data input.","type":"correction"}],"latest_changes":[{"description":"Updated classification of housing components in CPIH.","name":"Changes in classification","type":"Summary of changes"}],"links":{"dataset":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd","id":"cpih01"},"dimensions":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/dimensions"},"edition":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series","id":"time-series"},"job":{"href":"http://localhost:10700/jobs/383df410-845e-4efd-9ba1-ab469361eae5","id":"383df410-845e-4efd-9ba1-ab469361eae5"},"version":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series/versions/1","id":"1"},"spatial":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd"}},"release_date":"2025-01-15","state":"associated","themes":["Economy","Prices"],"temporal":[{"start_date":"2025-01-01","end_date":"2025-01-31","frequency":"Monthly"}],"usage_notes":[{"title":"Data usage guide","note":"This dataset is subject to revision and should be used in conjunction with the accompanying documentation."}]}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123//editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetNextVersionFunc: func(context.Context, string, string) (int, error) {
				return 2, nil
			},
			AddInstanceFunc: func(context.Context, *models.Instance) (*models.Instance, error) {
				return &models.Instance{InstanceID: "1234"}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.addDatasetVersionCondensed(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
		convey.So(mockedDataStore.CheckDatasetExistsCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.CheckEditionExistsCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetNextVersionCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.AddInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetDatasetCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpsertDatasetCalls(), convey.ShouldHaveLength, 1)
	})

	convey.Convey("When dataset does not exist", t, func() {
		b := `{"title":"test-dataset","description":"test dataset","type":"static","next_release":"2025-02-15","alerts":[{"date":"2025-01-15","description":"Correction to the CPIH index for December 2024 due to an error in data input.","type":"correction"}],"latest_changes":[{"description":"Updated classification of housing components in CPIH.","name":"Changes in classification","type":"Summary of changes"}],"links":{"dataset":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd","id":"cpih01"},"dimensions":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/dimensions"},"edition":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series","id":"time-series"},"job":{"href":"http://localhost:10700/jobs/383df410-845e-4efd-9ba1-ab469361eae5","id":"383df410-845e-4efd-9ba1-ab469361eae5"},"version":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series/versions/1","id":"1"},"spatial":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd"}},"release_date":"2025-01-15","state":"associated","themes":["Economy","Prices"],"temporal":[{"start_date":"2025-01-01","end_date":"2025-01-31","frequency":"Monthly"}],"usage_notes":[{"title":"Data usage guide","note":"This dataset is subject to revision and should be used in conjunction with the accompanying documentation."}]}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123//editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.addDatasetVersionCondensed(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
	})
	convey.Convey("When edition does not exist", t, func() {
		b := `{"title":"test-dataset","description":"test dataset","type":"static","next_release":"2025-02-15","alerts":[{"date":"2025-01-15","description":"Correction to the CPIH index for December 2024 due to an error in data input.","type":"correction"}],"latest_changes":[{"description":"Updated classification of housing components in CPIH.","name":"Changes in classification","type":"Summary of changes"}],"links":{"dataset":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd","id":"cpih01"},"dimensions":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/dimensions"},"edition":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series","id":"time-series"},"job":{"href":"http://localhost:10700/jobs/383df410-845e-4efd-9ba1-ab469361eae5","id":"383df410-845e-4efd-9ba1-ab469361eae5"},"version":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series/versions/1","id":"1"},"spatial":{"href":"http://localhost:10400/datasets/bara-test-ds-abcd"}},"release_date":"2025-01-15","state":"associated","themes":["Economy","Prices"],"temporal":[{"start_date":"2025-01-01","end_date":"2025-01-31","frequency":"Monthly"}],"usage_notes":[{"title":"Data usage guide","note":"This dataset is subject to revision and should be used in conjunction with the accompanying documentation."}]}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123//editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.addDatasetVersionCondensed(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
	})

	convey.Convey("When request body is not valid", t, func() {
		b := `{"title":"test-dataset","description":"test dataset","type":"static","next_release":"2025-02-15"}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123//editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetNextVersionFunc: func(context.Context, string, string) (int, error) {
				return 2, nil
			},
			AddInstanceFunc: func(context.Context, *models.Instance) (*models.Instance, error) {
				return &models.Instance{InstanceID: "1234"}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.addDatasetVersionCondensed(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
	})
}
