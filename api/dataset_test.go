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

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
	topicAPIModels "github.com/ONSdigital/dp-topic-api/models"
	topicAPISDK "github.com/ONSdigital/dp-topic-api/sdk"
	topicAPISDKErrors "github.com/ONSdigital/dp-topic-api/sdk/errors"

	cloudflareMocks "github.com/ONSdigital/dp-dataset-api/cloudflare/mocks"

	"github.com/ONSdigital/dp-authorisation/v2/authorisation"
	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/application"
	applicationMocks "github.com/ONSdigital/dp-dataset-api/application/mock"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	filesAPISDK "github.com/ONSdigital/dp-files-api/sdk"
	filesAPISDKMocks "github.com/ONSdigital/dp-files-api/sdk/mocks"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/gorilla/mux"

	topicAPISDKMocks "github.com/ONSdigital/dp-topic-api/sdk/mocks"

	"github.com/ONSdigital/dp-dataset-api/config"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host      = "http://localhost:22000"
	authToken = "dataset"
)

func boolPtr(b bool) *bool { return &b }

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
	datasetPayloadWithStatePublished           = `{"id":"123","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"static-published","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"StaticPublished","theme":"population","state":"published","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`
	datasetPayloadWithStateAssociated          = `{"id":"123","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"static-associated","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"StaticAssociated","theme":"population","state":"associated","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`
	datasetPayloadWithIDAndIsMigration         = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","keywords":["keyword"],"links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","id": "ageing-population-estimates", "next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"filterable","is_migration":false}`

	editionPayload = `"{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"`

	codeListAPIURL     = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL      = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL       = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	publicWebsiteURL   = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	privateWebsiteURL  = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	apiRouterPublicURL = &neturl.URL{Scheme: "http", Host: "localhost:23200", Path: "v1"}
	urlBuilder         = url.NewBuilder(publicWebsiteURL, privateWebsiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)
	enableURLRewriting = false
	mu                 sync.Mutex
)

func getSearchContentUpdatedMock() *mocks.KafkaProducerMock {
	producerMock := &mocks.KafkaProducerMock{
		OutputFunc: func() chan kafka.BytesMessage {
			return make(chan kafka.BytesMessage, 1)
		},
	}

	return producerMock
}

// GetAPIWithCMDMocks also used in other tests, so exported
func GetAPIWithCMDMocks(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, authorisationMock *authMock.MiddlewareMock, searchContentUpdated application.SearchContentUpdatedProducer, cloudflareMock *cloudflareMocks.ClienterMock, auditServiceMock *applicationMocks.AuditServiceMock, staticDatasetServiceMock *applicationMocks.StaticDatasetServiceMock, topicAPISDKMock *topicAPISDKMocks.ClienterMock, filesAPISDKMock *filesAPISDKMocks.ClienterMock) *DatasetAPI {
	mu.Lock()
	defer mu.Unlock()
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEndpoints = true
	cfg.DefaultLimit = 0
	cfg.DefaultOffset = 0
	cfg.EnableDeleteStaticVersion = true
	cfg.EnablePermissionsAuth = true
	cfg.CloudflareEnabled = true
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

	states := []application.State{application.Published, application.EditionConfirmed, application.Associated, application.Approved, application.PublishFailed}
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
			AllowedSourceStates: []string{"created", "associated", "approved"},
			Type:                "static",
		},
		{
			Label:               "published",
			TargetState:         application.Published,
			AllowedSourceStates: []string{"approved", "publish_failed"},
			Type:                "static",
		},
		{
			Label:               "approved",
			TargetState:         application.Approved,
			AllowedSourceStates: []string{"associated"},
			Type:                "static",
		},
		{
			Label:               "publish_failed",
			TargetState:         application.PublishFailed,
			AllowedSourceStates: []string{"approved"},
			Type:                "static",
		}}

	mockStatemachineDatasetAPI := application.StateMachineDatasetAPI{
		DataStore:                    store.DataStore{Backend: mockedDataStore},
		DownloadGenerators:           mockedMapSMGeneratedDownloads,
		StateMachine:                 application.NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore}),
		FilesAPIClient:               filesAPISDKMock,
		SearchContentUpdatedProducer: &searchContentUpdated,
		CloudflareEnabled:            cfg.CloudflareEnabled,
		UrlBuilder:                   urlBuilder,
		CloudflareClient:             cloudflareMock,
	}

	testIdentityClient := clientsidentity.New(cfg.ZebedeeURL)

	permissionsChecker := &authMock.PermissionsCheckerMock{
		HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
			return true, nil
		},
	}
	if authorisationMock.RequireWithAttributesFunc == nil {
		authorisationMock.RequireWithAttributesFunc = func(_ string, handler http.HandlerFunc, _ authorisation.GetAttributesFromRequest) http.HandlerFunc {
			return handler
		}
	}

	return Setup(testContext, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedMapGeneratedDownloads, authorisationMock, enableURLRewriting, &mockStatemachineDatasetAPI, auditServiceMock, staticDatasetServiceMock, permissionsChecker, testIdentityClient, filesAPISDKMock, topicAPISDKMock)
}

// GetAPIWithCMDMocks also used in other tests, so exported
func GetAPIWithCantabularMocks(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, authorisationMock *authMock.MiddlewareMock, cloudflareMock *cloudflareMocks.ClienterMock, auditServiceMock *applicationMocks.AuditServiceMock, staticDatasetServiceMock *applicationMocks.StaticDatasetServiceMock) *DatasetAPI {
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

	testIdentityClient := clientsidentity.New(cfg.ZebedeeURL)

	permissionsChecker := &authMock.PermissionsCheckerMock{}

	return Setup(testContext, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedMapGeneratedDownloads, authorisationMock, enableURLRewriting, &mockStatemachineDatasetAPI, auditServiceMock, staticDatasetServiceMock, permissionsChecker, testIdentityClient, &filesAPISDKMocks.ClienterMock{}, &topicAPISDKMocks.ClienterMock{})
}

func createRequestWithAuth(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r
}

func createRequestWithNoAuth(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	ctx := r.Context()
	r = r.WithContext(ctx)
	return r
}

func TestGetDatasetsUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("When request is unauthorised ", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets", http.NoBody)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 0)
	})
}

func TestGetDatasetsForbidden(t *testing.T) {
	t.Parallel()
	Convey("When the auth response is forbidden for get datasets, no database calls are made", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets", http.NoBody)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 0)
	})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)

		So(actualResponse, ShouldResemble, []*models.Dataset{})
		So(actualTotalCount, ShouldEqual, 15)
		So(err, ShouldEqual, nil)
		So(mockedDataStore.GetDatasetsCalls()[0].Limit, ShouldEqual, 11)
		So(mockedDataStore.GetDatasetsCalls()[0].Offset, ShouldEqual, 12)
	})

	Convey("A successful web-mode request to get datasets does not expose previous_series_id", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets")
		So(err, ShouldBeNil)
		r.URL = address

		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(context.Context, int, int, bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{{
					ID: "123-456",
					Current: &models.Dataset{
						ID:               "123-456",
						Type:             models.Static.String(),
						PreviousSeriesId: []string{"old-series-id"},
						IsMigration:      boolPtr(true),
					},
				}}, 1, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.enableURLRewriting = true

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)
		So(err, ShouldBeNil)
		So(actualTotalCount, ShouldEqual, 1)

		datasets, ok := actualResponse.([]*models.Dataset)
		So(ok, ShouldBeTrue)
		So(datasets, ShouldHaveLength, 1)
		So(datasets[0].PreviousSeriesId, ShouldBeNil)
		So(datasets[0].IsMigration, ShouldBeNil)
	})

	Convey("A successful web-mode request to get datasets does not expose previous_series_id when URL rewriting is disabled", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets")
		So(err, ShouldBeNil)
		r.URL = address

		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(context.Context, int, int, bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{{
					ID: "123-456",
					Current: &models.Dataset{
						ID:               "123-456",
						Type:             models.Static.String(),
						PreviousSeriesId: []string{"old-series-id"},
					},
				}}, 1, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.enableURLRewriting = false

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)
		So(err, ShouldBeNil)
		So(actualTotalCount, ShouldEqual, 1)

		datasets, ok := actualResponse.([]*models.Dataset)
		So(ok, ShouldBeTrue)
		So(datasets, ShouldHaveLength, 1)
		So(datasets[0].PreviousSeriesId, ShouldBeNil)
	})

	Convey("A successful request to get datasetwith type query parameter returns 200 OK response, and limit and offset are delegated to the datastore", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?type=static")
		So(err, ShouldBeNil)
		r.URL = address
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType, sortOrder, datasetID string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{{ID: "123-456", Current: &models.Dataset{ID: "123-456", Type: "static"}, Next: &models.Dataset{ID: "123-456", Type: "static"}}}, 1, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType, sortOrder, datasetID string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{{ID: "123-456", Current: &models.Dataset{ID: "123-456", Type: "static", IsBasedOn: &models.IsBasedOn{ID: "Example"}}, Next: &models.Dataset{ID: "123-456", Type: "static", IsBasedOn: &models.IsBasedOn{ID: "Example"}}}}, 1, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 11, 12)
		So(actualResponse, ShouldResemble, []*models.Dataset{{ID: "123-456", Type: "static", IsBasedOn: &models.IsBasedOn{ID: "Example"}}})
		So(actualTotalCount, ShouldEqual, 1)
		So(err, ShouldEqual, nil)
		So(mockedDataStore.GetDatasetsByQueryParamsCalls()[0].Limit, ShouldEqual, 11)
		So(mockedDataStore.GetDatasetsByQueryParamsCalls()[0].Offset, ShouldEqual, 12)
	})

	Convey("A successful request to get datasets with sort_order=ASC returns datasets sorted by ID a-z", t, func() {
		r := &http.Request{}
		w := httptest.NewRecorder()
		address, err := neturl.Parse("localhost:20000/datasets?sort_order=ASC")
		So(err, ShouldBeNil)
		r.URL = address

		mockedDataStore := &storetest.StorerMock{
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType, sortOrder, datasetID string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				So(sortOrder, ShouldEqual, "ASC")
				return []*models.DatasetUpdate{
					{ID: "a-dataset", Current: &models.Dataset{ID: "a-dataset"}},
					{ID: "m-dataset", Current: &models.Dataset{ID: "m-dataset"}},
					{ID: "z-dataset", Current: &models.Dataset{ID: "z-dataset"}},
				}, 3, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 10, 0)

		So(err, ShouldBeNil)
		So(actualTotalCount, ShouldEqual, 3)

		datasets, ok := actualResponse.([]*models.Dataset)
		So(ok, ShouldBeTrue)
		So(datasets, ShouldHaveLength, 3)

		So(datasets[0].ID, ShouldEqual, "a-dataset")
		So(datasets[1].ID, ShouldEqual, "m-dataset")
		So(datasets[2].ID, ShouldEqual, "z-dataset")
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 0)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 0)
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
			GetDatasetsByQueryParamsFunc: func(ctx context.Context, ID, datasetType, sortOrder, datasetID string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return nil, 0, errs.ErrDatasetTypeInvalid
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		actualResponse, actualTotalCount, err := api.getDatasets(w, r, 6, 7)

		So(len(mockedDataStore.GetDatasetsByQueryParamsCalls()), ShouldEqual, 1)
		So(actualResponse, ShouldResemble, nil)
		So(actualTotalCount, ShouldEqual, 0)
		So(err, ShouldEqual, errs.ErrDatasetTypeInvalid)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "invalid dataset type\n")
	})
}

func TestGetDatasetUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("When a request to retrieve a dataset is unauthorised", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
	})
}

func TestGetDatasetForbidden(t *testing.T) {
	t.Parallel()
	Convey("When a request to retrieve a dataset is forbidden", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("When dataset document has a current sub document return status 200", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}, Next: &models.Dataset{Type: models.Filterable.String()}}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(auditServiceMock.RecordDatasetAuditEventCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document and request is authorised return status 200", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123-456", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(auditServiceMock.RecordDatasetAuditEventCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has a current sub document return 200 (web mode)", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}, Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := GetWebAPIWithMocks(context.Background(), mockedDataStore, &mocks.DownloadsGeneratorMock{}, &authMock.MiddlewareMock{}, &authMock.PermissionsCheckerMock{}, &clientsidentity.Client{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When a web mode request gets a dataset, is_migration is not returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Current: &models.Dataset{ID: "123", IsMigration: boolPtr(true)},
					Next:    &models.Dataset{ID: "123", IsMigration: boolPtr(true)},
				}, nil
			},
		}

		api := GetWebAPIWithMocks(context.Background(), mockedDataStore, &mocks.DownloadsGeneratorMock{}, &authMock.MiddlewareMock{}, &authMock.PermissionsCheckerMock{}, &clientsidentity.Client{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{})
		api.enableURLRewriting = false
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(w.Body.String(), ShouldNotContainSubstring, "is_migration")
	})

	Convey("When a web mode request gets a dataset with URL rewriting enabled, is_migration is not returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Current: &models.Dataset{ID: "123", IsMigration: boolPtr(true)},
					Next:    &models.Dataset{ID: "123", IsMigration: boolPtr(true)},
				}, nil
			},
		}

		api := GetWebAPIWithMocks(context.Background(), mockedDataStore, &mocks.DownloadsGeneratorMock{}, &authMock.MiddlewareMock{}, &authMock.PermissionsCheckerMock{}, &clientsidentity.Client{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{})
		api.enableURLRewriting = true
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(w.Body.String(), ShouldNotContainSubstring, "is_migration")
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document return status 404 (web mode)", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := GetWebAPIWithMocks(context.Background(), mockedDataStore, &mocks.DownloadsGeneratorMock{}, &authMock.MiddlewareMock{}, &authMock.PermissionsCheckerMock{}, &clientsidentity.Client{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When there is no dataset document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When the AuditService returns an error when logging the audit event, return 500", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return errors.New("failed to record audit event")
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(auditServiceMock.RecordDatasetAuditEventCalls()), ShouldEqual, 1)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request does not contain a token returns 401", t, func() {
		b := datasetPayload
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request does not contain a valid internal token returns 403", t, func() {
		b := datasetPayload
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset already exists and a request is sent to create the same dataset return status conflict", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(w.Body.String(), ShouldResemble, "dataset already exists\n")
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
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

func TestAddDatasetNew(t *testing.T) {
	Convey("A successful request to post a dataset returns 201 Created response", t, func() {
		b := datasetPayloadWithID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)

		So(len(authorisationMock.ParseCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)
		So(len(auditServiceMock.RecordDatasetAuditEventCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("Request with empty dataset ID returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Request with empty dataset title returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyTitle
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Request with empty dataset description returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyDescription
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Request with empty dataset next release returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyNextRelease
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Request with empty dataset keywords returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyKeywords
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Request with empty topics returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyTopicsAndTypeStatic
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Request with empty dataset contacts returns 400 Bad Request", t, func() {
		b := datasetPayloadWithEmptyContacts
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("When the request contains duplicate dataset title a conflict request status is returned", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When AuditService returns and error", t, func() {
		b := datasetPayloadWithID
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return errors.New("audit error")
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)
		So(len(auditServiceMock.RecordDatasetAuditEventCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When POST dataset calls with id with spaces returns 400 response", t, func() {
		b := `{"id":"id with spaces","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("A request to post a dataset with is_migration false stores the value", t, func() {
		b := datasetPayloadWithIDAndIsMigration
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls()[0].DatasetDoc.Next.IsMigration, ShouldNotBeNil)
		So(*mockedDataStore.UpsertDatasetCalls()[0].DatasetDoc.Next.IsMigration, ShouldBeFalse)
	})
}

func TestPutDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("A successful request to put dataset with type static and state published returns 200 OK response", t, func() {
		b := datasetPayloadWithStatePublished
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
					Next: &models.Dataset{
						Type:   models.Static.String(),
						Topics: []string{"topic-0"},
					}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})

		Convey("and the response body contains the dataset we sent with the request", func() {
			var expected, actual models.Dataset

			err := json.Unmarshal([]byte(datasetPayloadWithStatePublished), &expected)
			So(err, ShouldBeNil)

			err = json.Unmarshal(w.Body.Bytes(), &actual)
			So(err, ShouldBeNil)

			So(actual, ShouldResemble, expected)
		})
	})
	Convey("A successful request to put dataset with type static and state associated returns 200 OK response", t, func() {
		b := datasetPayloadWithStateAssociated
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
					Next: &models.Dataset{
						Type:   models.Static.String(),
						Topics: []string{"topic-0"},
					}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})

		Convey("and the response body contains the dataset we sent with the request", func() {
			var expected, actual models.Dataset

			err := json.Unmarshal([]byte(datasetPayloadWithStateAssociated), &expected)
			So(err, ShouldBeNil)

			err = json.Unmarshal(w.Body.Bytes(), &actual)
			So(err, ShouldBeNil)

			So(actual, ShouldResemble, expected)
		})
	})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
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
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: "static", Topics: []string{"topic-0", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
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

	Convey("A successful request to rename an unpublished static dataset returns 200 OK response", t, func() {
		b := `{"id":"456","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0","previous_series_id":["789"]}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"topic-0", "topic-1"}, PreviousSeriesId: []string{"789"}}}, nil
			},
			CheckDatasetExistsFunc: func(ctx context.Context, id, state string) error {
				return errs.ErrDatasetNotFound
			},
			GetVersionsStaticNoLimitFunc: func(ctx context.Context, datasetID, state string) ([]*models.Version, int, error) {
				return []*models.Version{}, 0, errs.ErrVersionsNotFound
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetVersionsStaticNoLimitCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls()[0].ID, ShouldEqual, "456")
		So(mockedDataStore.UpsertDatasetCalls()[0].DatasetDoc.Next.PreviousSeriesId, ShouldResemble, []string{"789", "123"})
		So(mockedDataStore.DeleteDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)
	})

	Convey("A successful request to put static dataset with canonical topic change updates version web_page links", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		updatedWebPageHref := ""

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"old-topic", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			GetVersionsStaticNoLimitFunc: func(ctx context.Context, datasetID, state string) ([]*models.Version, int, error) {
				return []*models.Version{{
					Edition: "2025",
					Version: 1,
					ETag:    "etag-1",
					Links:   &models.VersionLinks{WebPage: &models.LinkObject{HRef: "/old/web/page"}},
				}}, 1, nil
			},
			UpdateVersionStaticFunc: func(ctx context.Context, currentVersion *models.Version, versionUpdate *models.Version, eTagSelector string) (string, error) {
				updatedWebPageHref = versionUpdate.Links.WebPage.HRef
				return "new-etag", nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		topicAPIMock := &topicAPISDKMocks.ClienterMock{
			GetTopicPrivateFunc: func(ctx context.Context, reqHeaders topicAPISDK.Headers, id string) (*topicAPIModels.TopicResponse, topicAPISDKErrors.Error) {
				return &topicAPIModels.TopicResponse{Current: &topicAPIModels.Topic{ID: id, Slug: "businessindustryandtrade"}}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, topicAPIMock, &filesAPISDKMocks.ClienterMock{})
		api.topicAPIClient = topicAPIMock
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(updatedWebPageHref, ShouldEqual, "businessindustryandtrade/datasets/123/editions/2025/versions/1")
		So(mockedDataStore.GetVersionsStaticNoLimitCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 1)
		So(topicAPIMock.GetTopicPrivateCalls(), ShouldHaveLength, 1)
	})

	Convey("When put static dataset topic is unchanged, version web_page links are not updated", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"topic-0", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, &topicAPISDKMocks.ClienterMock{}, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetEditionsStaticCalls(), ShouldHaveLength, 0)
		So(mockedDataStore.GetVersionsStaticCalls(), ShouldHaveLength, 0)
	})

	Convey("When put static dataset has a Current sub-document (published), topic change path for version updates is skipped", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				// Current is non-nil so the unpublished topic-change branch is skipped
				return &models.DatasetUpdate{
					ID:      "123",
					Current: &models.Dataset{Type: models.Static.String(), State: models.PublishedState, Topics: []string{"old-topic", "topic-1"}},
					Next:    &models.Dataset{Type: models.Static.String(), State: models.PublishedState, Topics: []string{"old-topic", "topic-1"}},
				}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, &topicAPISDKMocks.ClienterMock{}, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		// published dataset with changed canonical topic should return 409
		So(w.Code, ShouldEqual, http.StatusConflict)
		So(mockedDataStore.GetEditionsStaticCalls(), ShouldHaveLength, 0)
		So(mockedDataStore.GetVersionsStaticCalls(), ShouldHaveLength, 0)
	})

	Convey("When put static dataset topic changes but a dataset has no versions, no UpdateVersionStatic calls are made", t, func() {
		b := datasetPayloadWithTypeStatic
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"old-topic", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			GetVersionsStaticNoLimitFunc: func(ctx context.Context, datasetID, state string) ([]*models.Version, int, error) {
				return []*models.Version{}, 0, errs.ErrVersionsNotFound
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		topicAPIMock := &topicAPISDKMocks.ClienterMock{
			GetTopicPrivateFunc: func(ctx context.Context, reqHeaders topicAPISDK.Headers, id string) (*topicAPIModels.TopicResponse, topicAPISDKErrors.Error) {
				return &topicAPIModels.TopicResponse{Current: &topicAPIModels.Topic{ID: id, Slug: "newslug"}}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, topicAPIMock, &filesAPISDKMocks.ClienterMock{})
		api.topicAPIClient = topicAPIMock
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetVersionsStaticNoLimitCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 0)
		So(topicAPIMock.GetTopicPrivateCalls(), ShouldHaveLength, 0)
	})

	Convey("When put static dataset topic changes and GetVersionsStaticNoLimit returns error, 500 is returned", t, func() {
		b := datasetPayloadWithTypeStatic // topics ["topic-0","topic-1"]
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"old-topic", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			GetVersionsStaticNoLimitFunc: func(ctx context.Context, datasetID, state string) ([]*models.Version, int, error) {
				return nil, 0, errors.New("versions store error")
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		topicAPIMock := &topicAPISDKMocks.ClienterMock{
			GetTopicPrivateFunc: func(ctx context.Context, reqHeaders topicAPISDK.Headers, id string) (*topicAPIModels.TopicResponse, topicAPISDKErrors.Error) {
				return &topicAPIModels.TopicResponse{Current: &topicAPIModels.Topic{ID: id, Slug: "newslug"}}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, topicAPIMock, &filesAPISDKMocks.ClienterMock{})
		api.topicAPIClient = topicAPIMock
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(mockedDataStore.GetVersionsStaticNoLimitCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 0)
	})

	Convey("When put static dataset topic changes and GetTopicPrivate returns error, 500 is returned", t, func() {
		b := datasetPayloadWithTypeStatic // topics ["topic-0","topic-1"]
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"old-topic", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			GetVersionsStaticNoLimitFunc: func(ctx context.Context, datasetID, state string) ([]*models.Version, int, error) {
				return []*models.Version{{Edition: "2025", Version: 1, ETag: "e1", Links: &models.VersionLinks{WebPage: &models.LinkObject{HRef: "/old"}}}}, 1, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		topicAPIMock := &topicAPISDKMocks.ClienterMock{
			GetTopicPrivateFunc: func(ctx context.Context, reqHeaders topicAPISDK.Headers, id string) (*topicAPIModels.TopicResponse, topicAPISDKErrors.Error) {
				return nil, topicAPISDKErrors.StatusError{Code: 500}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, topicAPIMock, &filesAPISDKMocks.ClienterMock{})
		api.topicAPIClient = topicAPIMock
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(topicAPIMock.GetTopicPrivateCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 0)
	})

	Convey("When put static dataset topic changes and UpdateVersionStatic returns error, 500 is returned", t, func() {
		b := datasetPayloadWithTypeStatic // topics ["topic-0","topic-1"]
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"old-topic", "topic-1"}}}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
			GetVersionsStaticNoLimitFunc: func(ctx context.Context, datasetID, state string) ([]*models.Version, int, error) {
				return []*models.Version{{Edition: "2025", Version: 1, ETag: "e1", Links: &models.VersionLinks{WebPage: &models.LinkObject{HRef: "/old"}}}}, 1, nil
			},
			UpdateVersionStaticFunc: func(ctx context.Context, currentVersion *models.Version, versionUpdate *models.Version, eTagSelector string) (string, error) {
				return "", errors.New("update version store error")
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		topicAPIMock := &topicAPISDKMocks.ClienterMock{
			GetTopicPrivateFunc: func(ctx context.Context, reqHeaders topicAPISDK.Headers, id string) (*topicAPIModels.TopicResponse, topicAPISDKErrors.Error) {
				return &topicAPIModels.TopicResponse{Current: &topicAPIModels.Topic{ID: id, Slug: "newslug"}}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, topicAPIMock, &filesAPISDKMocks.ClienterMock{})
		api.topicAPIClient = topicAPIMock
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 1)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrAddUpdateDatasetBadRequest
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

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
		b := datasetPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request is forbidden to update dataset return status forbidden", t, func() {
		b := datasetPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-viewer"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When updating the static dataset with a title that already exists returns 409 conflict", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(w.Body.String(), ShouldResemble, "dataset title already exists\n")
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetTitleExistCalls()), ShouldEqual, 1)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When PUT dataset calls with id with spaces returns 400 response", t, func() {
		b := `{"id":"id with spaces","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`

		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:   "123",
					Next: &models.Dataset{Type: models.Static.String()}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("When PUT dataset calls trying to change canonical topic returns 409 response", t, func() {
		b := datasetPayloadWithStatePublished
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Current: &models.Dataset{Type: models.Static.String(), State: models.PublishedState, Topics: []string{"topic-change", "topic-1"}},
					Next:    &models.Dataset{Type: models.Static.String(), Title: "StaticPublished", Topics: []string{"topic-0", "topic-1"}},
				}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When PUT static published dataset calls trying to change id returns 409 response", t, func() {
		b := `{"id":"456","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"static-published","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"StaticPublished","theme":"population","state":"published","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Current: &models.Dataset{Type: models.Static.String(), State: models.PublishedState, Topics: []string{"topic-0", "topic-1"}},
					Next:    &models.Dataset{Type: models.Static.String(), Title: "StaticPublished", Topics: []string{"topic-0", "topic-1"}},
				}, nil
			},
			CheckDatasetTitleExistFunc: func(ctx context.Context, title string) (bool, error) {
				return false, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{}
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)
		So(mockedDataStore.DeleteDatasetCalls(), ShouldHaveLength, 0)
	})

	Convey("When a request is made to change the dataset id of a migrated dataset", t, func() {
		b := `{"id":"456","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"static-published","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"StaticPublished","theme":"population","state":"published","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Current: &models.Dataset{Type: models.Static.String(), State: models.PublishedState, Topics: []string{"topic-0", "topic-1"}, IsMigration: new(true)},
					Next:    &models.Dataset{Type: models.Static.String(), Title: "StaticPublished", Topics: []string{"topic-0", "topic-1"}, IsMigration: new(true)},
				}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		Convey("Then it should return a 409 conflict response", func() {
			So(w.Code, ShouldEqual, http.StatusConflict)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrCannotChangeDatasetIDForMigratedDataset.Error())

			So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		})
	})

	Convey("When PUT static unpublished dataset calls trying to change id to an existing one returns 409 response", t, func() {
		b := `{"id":"456","contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","href":"https://www.ons.gov.uk/"},"type":"static","keywords":["keyword","keyword 2"],"topics":["topic-0","topic-1"],"license":"Open Government Licence v3.0"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{Type: models.Static.String(), Title: "CensusEthnicity", State: models.CreatedState, Topics: []string{"topic-0", "topic-1"}}}, nil
			},
			CheckDatasetExistsFunc: func(ctx context.Context, id, state string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{}
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)
		So(mockedDataStore.DeleteDatasetCalls(), ShouldHaveLength, 0)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls(), ShouldHaveLength, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Action, ShouldEqual, models.ActionDelete)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Resource, ShouldEqual, "/datasets/123")
	})

	Convey("A successful request to delete dataset with editions returns 200 OK response", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return []*models.EditionUpdate{{}}, 0, nil
			},
			DeleteEditionFunc: func(context.Context, string) error {
				return nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls(), ShouldHaveLength, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Action, ShouldEqual, models.ActionDelete)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Resource, ShouldEqual, "/datasets/123")
	})

	Convey("A successful request to delete a static dataset with versions returns 204 No Content", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/456", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "456",
					Next: &models.Dataset{
						State: models.CreatedState,
						Type:  models.Static.String(),
					},
				}, nil
			},
			GetVersionsStaticNoLimitFunc: func(context.Context, string, string) ([]*models.Version, int, error) {
				versions := []*models.Version{
					{
						ID: "V1",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								ID: "456",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution1",
								DownloadURL: "path/to/distribution1.txt",
							},
							{
								Title:       "Distribution2",
								DownloadURL: "path/to/distribution2.txt",
							},
						},
					},
					{
						ID: "V2",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								ID: "456",
							},
						},
					},
				}
				return versions, 1, nil
			},
			DeleteStaticDatasetVersionFunc: func(ctx context.Context, datasetID, editionID string, version int) error {
				return nil
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		mockFilesAPIClient := filesAPISDKMocks.ClienterMock{
			DeleteFileFunc: func(ctx context.Context, filePath string, headers filesAPISDK.Headers) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.filesAPIClient = &mockFilesAPIClient

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 1)
		So(len(mockFilesAPIClient.DeleteFileCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.DeleteStaticDatasetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls(), ShouldHaveLength, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Action, ShouldEqual, models.ActionDelete)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Resource, ShouldEqual, "/datasets/456")
	})

	Convey("A successful request to delete a static dataset with no versions returns 204 No Content", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/456", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "456",
					Next: &models.Dataset{
						State: models.CreatedState,
						Type:  models.Static.String(),
					},
				}, nil
			},
			GetVersionsStaticNoLimitFunc: func(context.Context, string, string) ([]*models.Version, int, error) {
				version := []*models.Version{}
				return version, 0, errs.ErrVersionsNotFound
			},
			DeleteDatasetFunc: func(context.Context, string) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls(), ShouldHaveLength, 1)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Action, ShouldEqual, models.ActionDelete)
		So(auditServiceMock.RecordDatasetAuditEventCalls()[0].Resource, ShouldEqual, "/datasets/456")
	})
}

func TestDeleteDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When recording a delete dataset audit event fails, return an internal server error", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordDatasetAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, dataset *models.Dataset) error {
				return errors.New("failed to record audit event")
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(auditServiceMock.RecordDatasetAuditEventCalls(), ShouldHaveLength, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})

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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request is not authorised to delete the dataset return unauthorised and no database calls are made", t, func() {
		b := editionPayload
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request is forbidden to delete the dataset return forbidden and no database calls are made", t, func() {
		b := editionPayload
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When deleting a static dataset fails at DeleteStaticDatasetVersion, return internal server error", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/456", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{
						ID:    "456",
						Type:  models.Static.String(),
						State: models.CreatedState,
					},
				}, nil
			},
			GetVersionsStaticNoLimitFunc: func(context.Context, string, string) ([]*models.Version, int, error) {
				versions := []*models.Version{
					{
						ID: "V1",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								ID: "456",
							},
						},
					},
					{
						ID: "V2",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								ID: "456",
							},
						},
					},
				}
				return versions, len(versions), nil
			},
			DeleteStaticDatasetVersionFunc: func(ctx context.Context, datasetID, editionID string, version int) error {
				return errs.ErrInternalServer
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteStaticDatasetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When deleting a static dataset fails at DeleteFile, return internal server error", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/456", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{
						ID:    "456",
						Type:  models.Static.String(),
						State: models.CreatedState,
					},
				}, nil
			},
			GetVersionsStaticNoLimitFunc: func(context.Context, string, string) ([]*models.Version, int, error) {
				versions := []*models.Version{
					{
						ID: "1",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								ID: "456",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution1",
								DownloadURL: "path/to/distribution1.txt",
							},
						},
					},
				}
				return versions, len(versions), nil
			},
		}

		mockFilesAPIClient := filesAPISDKMocks.ClienterMock{
			DeleteFileFunc: func(ctx context.Context, filePath string, headers filesAPISDK.Headers) error {
				return errors.New("files api returned an error")
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, application.SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{}, &applicationMocks.StaticDatasetServiceMock{}, nil, &filesAPISDKMocks.ClienterMock{})
		api.filesAPIClient = &mockFilesAPIClient
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 1)
		So(len(mockFilesAPIClient.DeleteFileCalls()), ShouldEqual, 1)
	})
}
