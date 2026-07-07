package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	healthcheck "github.com/ONSdigital/dp-api-clients-go/v2/health"
	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
)

var (
	testEntityData = &permissionsAPISDK.EntityData{
		UserID: "user-1",
		Groups: []string{"group1", "group2"},
	}

	testServiceEntityData = &permissionsAPISDK.EntityData{
		UserID: "service-1",
	}
)

func newMockHTTPClient(retCode int, retBody interface{}) *dphttp.ClienterMock {
	return &dphttp.ClienterMock{
		SetPathsWithNoRetriesFunc: func(paths []string) {},
		GetPathsWithNoRetriesFunc: func() []string {
			return []string{"/healthcheck"}
		},
		DoFunc: func(ctx context.Context, req *http.Request) (*http.Response, error) {
			body, _ := json.Marshal(retBody)
			return &http.Response{
				StatusCode: retCode,
				Body:       io.NopCloser(bytes.NewReader(body)),
			}, nil
		},
	}
}

var testIdentityResponse = &dprequest.IdentityResponse{
	Identifier: "myIdentity",
}

var testServiceIdentityResponse = &dprequest.IdentityResponse{
	Identifier: "service-1",
}

func TestGetAuthEntityData(t *testing.T) {
	Convey("Given a DatasetAPI instance with a mocked auth middleware", t, func() {
		mockAuthMiddleware := &authMock.MiddlewareMock{
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				if token == "valid-token" {
					return testEntityData, nil
				}
				return nil, errors.New("parse error")
			},
		}

		httpClient := newMockHTTPClient(200, testIdentityResponse)
		idClient := clientsidentity.NewWithHealthClient(healthcheck.NewClientWithClienter("", "http://localhost:8082", httpClient))

		api := &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       idClient,
		}

		rValid := http.Request{
			Header: http.Header{},
		}
		rValid.Header.Set("Authorization", "Bearer valid-token")

		Convey("When getAuthEntityData is called with a valid access token", func() {
			entityData, err := api.getAuthEntityData(&rValid)

			Convey("Then it should return the expected EntityData and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldResemble, &AuthEntityData{
					EntityData:    testEntityData,
					IsServiceAuth: false,
				})
			})
		})

		httpClientUnauthorised := newMockHTTPClient(403, testIdentityResponse)
		idClientUnauthorised := clientsidentity.NewWithHealthClient(healthcheck.NewClientWithClienter("", "http://localhost:8082", httpClientUnauthorised))

		api = &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       idClientUnauthorised,
		}

		rNotValid := http.Request{
			Header: http.Header{},
		}
		rNotValid.Header.Set("Authorization", "Bearer invalid-token")

		Convey("When getAuthEntityData is called with an invalid access token", func() {
			entityData, err := api.getAuthEntityData(&rNotValid)

			Convey("Then it should return an error indicating a parse failure", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "failed to parse access token: unexpected status code returned from AuthAPI: unable to determine the user or service making the request")
				So(entityData, ShouldBeNil)
			})
		})
	})

	Convey("Given a DatasetAPI instance with a mocked auth middleware that fails jwt auth", t, func() {
		mockAuthMiddleware := &authMock.MiddlewareMock{
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, errors.New("parse error")
			},
		}

		httpClient := newMockHTTPClient(200, testServiceIdentityResponse)
		idClient := clientsidentity.NewWithHealthClient(healthcheck.NewClientWithClienter("", "http://localhost:8082", httpClient))

		api := &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       idClient,
		}

		rValid := http.Request{
			Header: http.Header{},
		}
		rValid.Header.Set("Authorization", "Bearer valid-service-token")

		Convey("When getAuthEntityData is called with a valid service token", func() {
			entityData, err := api.getAuthEntityData(&rValid)

			Convey("Then it should return the expected EntityData and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldResemble, &AuthEntityData{
					EntityData:    testServiceEntityData,
					IsServiceAuth: true,
				})
			})
		})
	})
}

func TestGetAccessTokenFromRequest(t *testing.T) {
	testCases := []struct {
		name                string
		authorizationHeader string
		expectedToken       string
	}{
		{
			name:                "Valid Bearer token",
			authorizationHeader: "Bearer valid-token",
			expectedToken:       "valid-token",
		},
		{
			name:                "No Bearer prefix",
			authorizationHeader: "valid-token",
			expectedToken:       "valid-token",
		},
		{
			name:                "Empty Authorization header",
			authorizationHeader: "",
			expectedToken:       "",
		},
	}

	for _, tc := range testCases {
		Convey("Given an HTTP request with "+tc.name, t, func() {
			req, err := http.NewRequest("GET", "http://example.com", http.NoBody)
			So(err, ShouldBeNil)

			req.Header.Set(dprequest.AuthHeaderKey, tc.authorizationHeader)

			Convey("When getAccessTokenFromRequest is called", func() {
				token := getAccessTokenFromRequest(req)

				Convey("Then it should return the expected access token", func() {
					So(token, ShouldEqual, tc.expectedToken)
				})
			})
		})
	}
}

func TestGetPermissionAttributesFromRequest(t *testing.T) {
	Convey("Given a request for permission attributes", t, func() {
		Convey("When only a dataset id is provided", func() {
			datasetID := "test-dataset"
			mockedDataStore := &storetest.StorerMock{}
			api := DatasetAPI{dataStore: store.DataStore{Backend: mockedDataStore}}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID, http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return the dataset id without checking versions", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID})
				So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 0)
			})
		})

		Convey("When a dataset id and edition are provided", func() {
			datasetID := "test-dataset"
			edition := "2024"
			mockedDataStore := &storetest.StorerMock{
				GetVersionsStaticNoLimitFunc: func(ctx context.Context, requestedDatasetID string, state string) ([]*models.Version, int, error) {
					return []*models.Version{}, 0, nil
				},
			}
			api := DatasetAPI{dataStore: store.DataStore{Backend: mockedDataStore}}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID+"/editions/"+edition, http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID, "edition": edition})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return the dataset edition from the request", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + edition})
				So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 1)
			})
		})

		Convey("When a dataset id and edition are provided and the user has access to a previous edition", func() {
			datasetID := "test-dataset"
			edition := "2024"
			previousEdition := "2023"
			mockedDataStore := &storetest.StorerMock{
				GetVersionsStaticNoLimitFunc: func(ctx context.Context, requestedDatasetID string, state string) ([]*models.Version, int, error) {
					return []*models.Version{{PreviousEditionId: []string{previousEdition}}}, 0, nil
				},
			}
			permissionsChecker := &authMock.PermissionsCheckerMock{
				HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
					return attributes["dataset_edition"] == datasetID+"/"+previousEdition, nil
				},
			}
			api := DatasetAPI{
				dataStore:            store.DataStore{Backend: mockedDataStore},
				EnablePrePublishView: true,
				authMiddleware: &authMock.MiddlewareMock{
					ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
						So(token, ShouldEqual, "valid-token")
						return testEntityData, nil
					},
				},
				permissionsChecker: permissionsChecker,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID+"/editions/"+edition, http.NoBody)
			req.Header.Set(dprequest.AuthHeaderKey, dprequest.BearerPrefix+"valid-token")
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID, "edition": edition})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return the permitted previous edition", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + previousEdition})
				So(len(mockedDataStore.GetVersionsStaticNoLimitCalls()), ShouldEqual, 1)
				So(len(permissionsChecker.HasPermissionCalls()), ShouldEqual, 1)
				So(permissionsChecker.HasPermissionCalls()[0].Attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + previousEdition})
			})
		})
	})
}
