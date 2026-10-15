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
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
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
	Convey("Given a request for permission attributes in publishing mode", t, func() {
		Convey("When only a dataset id is provided and the dataset has no previous series id", func() {
			datasetID := "test-dataset"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{ID: datasetID, Next: &models.Dataset{ID: datasetID}}, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID, http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return the dataset id without checking versions", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID})
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionsStaticByEditionNoLimitCalls()), ShouldEqual, 0)
			})
		})

		Convey("When only a dataset id is provided and the dataset's Next document is nil", func() {
			datasetID := "test-dataset"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{ID: datasetID}, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID, http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should fall back to the dataset id", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID})
			})
		})

		Convey("When only a dataset id is provided and GetDataset returns an error", func() {
			datasetID := "test-dataset"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
				GetDatasetByPreviousSeriesIDFunc: func(ctx context.Context, previousSeriesID string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID, http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should fall back to the dataset id and not return an error", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID})
			})
		})

		Convey("When a dataset id is provided and the user has permission on a previous series id", func() {
			datasetID := "test-series-b"
			previousSeriesID := "test-series-a"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{
						ID:   datasetID,
						Next: &models.Dataset{ID: datasetID, PreviousSeriesId: []string{previousSeriesID}},
					}, nil
				},
			}
			permissionsChecker := &authMock.PermissionsCheckerMock{
				HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
					return attributes["dataset_edition"] == previousSeriesID, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
				EnablePrePublishView:   true,
				authMiddleware: &authMock.MiddlewareMock{
					ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
						So(token, ShouldEqual, "valid-token")
						return testEntityData, nil
					},
				},
				permissionsChecker: permissionsChecker,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID, http.NoBody)
			req.Header.Set(dprequest.AuthHeaderKey, dprequest.BearerPrefix+"valid-token")
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return the permitted previous series id", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": previousSeriesID})
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(permissionsChecker.HasPermissionCalls()), ShouldEqual, 2)
				So(permissionsChecker.HasPermissionCalls()[0].Attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID})
				So(permissionsChecker.HasPermissionCalls()[1].Attributes, ShouldResemble, map[string]string{"dataset_edition": previousSeriesID})
			})
		})

		Convey("When a dataset id is provided and the user does not have permission on any previous series id", func() {
			datasetID := "test-series-b"
			previousSeriesID := "test-series-a"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{
						ID:   datasetID,
						Next: &models.Dataset{ID: datasetID, PreviousSeriesId: []string{previousSeriesID}},
					}, nil
				},
			}
			permissionsChecker := &authMock.PermissionsCheckerMock{
				HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
					return false, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
				EnablePrePublishView:   true,
				authMiddleware: &authMock.MiddlewareMock{
					ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
						return testEntityData, nil
					},
				},
				permissionsChecker: permissionsChecker,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID, http.NoBody)
			req.Header.Set(dprequest.AuthHeaderKey, dprequest.BearerPrefix+"valid-token")
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should fall back to the current dataset id", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID})
				So(len(permissionsChecker.HasPermissionCalls()), ShouldEqual, 2)
			})
		})

		Convey("When a dataset id and edition are provided", func() {
			datasetID := "test-dataset"
			edition := "2024"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{ID: datasetID, Next: &models.Dataset{ID: datasetID}}, nil
				},
				GetVersionsStaticByEditionNoLimitFunc: func(ctx context.Context, datasetID string, edition string, state string) ([]*models.Version, int, error) {
					return []*models.Version{}, 0, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
			}

			req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID+"/editions/"+edition, http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"dataset_id": datasetID, "edition": edition})

			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return the dataset edition from the request", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + edition})
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionsStaticByEditionNoLimitCalls()), ShouldEqual, 1)
			})
		})

		Convey("When a dataset id and edition are provided and the user has access to a previous edition", func() {
			datasetID := "test-dataset"
			edition := "2024"
			previousEdition := "2023"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{ID: datasetID, Next: &models.Dataset{ID: datasetID}}, nil
				},
				GetVersionsStaticByEditionNoLimitFunc: func(ctx context.Context, datasetID string, edition string, state string) ([]*models.Version, int, error) {
					return []*models.Version{{PreviousEditionId: []string{previousEdition}}}, 0, nil
				},
			}
			permissionsChecker := &authMock.PermissionsCheckerMock{
				HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
					return attributes["dataset_edition"] == datasetID+"/"+previousEdition, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
				EnablePrePublishView:   true,
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
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionsStaticByEditionNoLimitCalls()), ShouldEqual, 1)
				So(len(permissionsChecker.HasPermissionCalls()), ShouldEqual, 2)
				So(permissionsChecker.HasPermissionCalls()[0].Attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + edition})
				So(permissionsChecker.HasPermissionCalls()[1].Attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + previousEdition})
			})
		})

		Convey("When a dataset id and edition are provided and the user has access to a previous series id and a previous series id", func() {
			datasetID := "test-series-b"
			edition := "2024"
			previousSeriesID := "test-series-a"
			previousEdition := "2023"
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{
						ID: datasetID,
						Next: &models.Dataset{
							ID:               datasetID,
							PreviousSeriesId: []string{previousSeriesID},
						},
					}, nil
				},
				GetVersionsStaticByEditionNoLimitFunc: func(ctx context.Context, datasetID string, edition string, state string) ([]*models.Version, int, error) {
					return []*models.Version{{PreviousEditionId: []string{previousEdition}}}, 0, nil
				},
			}
			permissionsChecker := &authMock.PermissionsCheckerMock{
				HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
					return attributes["dataset_edition"] == previousSeriesID+"/"+previousEdition, nil
				},
			}
			api := DatasetAPI{
				dataStore:              store.DataStore{Backend: mockedDataStore},
				enablePrivateEndpoints: true,
				EnablePrePublishView:   true,
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

			Convey("Then it should return the permitted previous series and previous edition", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": previousSeriesID + "/" + previousEdition})
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionsStaticByEditionNoLimitCalls()), ShouldEqual, 1)
				So(len(permissionsChecker.HasPermissionCalls()), ShouldEqual, 4)
				So(permissionsChecker.HasPermissionCalls()[0].Attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + edition})
				So(permissionsChecker.HasPermissionCalls()[1].Attributes, ShouldResemble, map[string]string{"dataset_edition": datasetID + "/" + previousEdition})
				So(permissionsChecker.HasPermissionCalls()[2].Attributes, ShouldResemble, map[string]string{"dataset_edition": previousSeriesID + "/" + edition})
				So(permissionsChecker.HasPermissionCalls()[3].Attributes, ShouldResemble, map[string]string{"dataset_edition": previousSeriesID + "/" + previousEdition})
			})
		})
	})

	Convey("Given a request for permission attributes in web mode", t, func() {
		api := DatasetAPI{
			enablePrivateEndpoints: false,
		}

		req := httptest.NewRequest(http.MethodGet, "/datasets/test-dataset", http.NoBody)

		Convey("When GetPermissionAttributesFromRequest is called", func() {
			attributes, err := api.getPermissionAttributesFromRequest(req)

			Convey("Then it should return nil attributes and no error", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldBeNil)
			})
		})
	})
}

func TestGetPermissionAttributesFromRequestPreviousEditionID(t *testing.T) {
	renamedDataset := &models.DatasetUpdate{
		ID:   "new-series",
		Next: &models.Dataset{ID: "new-series", Type: models.Static.String(), PreviousSeriesId: []string{"old-series"}},
	}

	newMockedDataStore := func() *storetest.StorerMock {
		return &storetest.StorerMock{
			GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
				if id == "new-series" {
					return renamedDataset, nil
				}
				return nil, errs.ErrDatasetNotFound
			},
			GetDatasetByPreviousSeriesIDFunc: func(ctx context.Context, previousSeriesID string) (*models.DatasetUpdate, error) {
				if previousSeriesID == "old-series" {
					return renamedDataset, nil
				}
				return nil, errs.ErrDatasetNotFound
			},
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				if datasetID == "new-series" && editionID == "new-edition" {
					return nil
				}
				return errs.ErrEditionNotFound
			},
			GetVersionsStaticByPreviousEditionIDFunc: func(ctx context.Context, datasetID, previousEditionID, state string, offset, limit int) ([]models.Version, int, error) {
				if datasetID == "new-series" && previousEditionID == "old-edition" {
					return []models.Version{{Edition: "new-edition", PreviousEditionId: []string{"old-edition"}}}, 1, nil
				}
				return nil, 0, errs.ErrVersionNotFound
			},
			GetVersionsStaticByEditionNoLimitFunc: func(ctx context.Context, datasetID, edition, state string) ([]*models.Version, int, error) {
				if datasetID == "new-series" && edition == "new-edition" {
					return []*models.Version{{Edition: "new-edition", PreviousEditionId: []string{"old-edition"}}}, 1, nil
				}
				return nil, 0, errs.ErrVersionsNotFound
			},
		}
	}

	newAPI := func(mockedDataStore *storetest.StorerMock, permittedAttribute string) (*DatasetAPI, *authMock.PermissionsCheckerMock) {
		permissionsChecker := &authMock.PermissionsCheckerMock{
			HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
				return attributes["dataset_edition"] == permittedAttribute, nil
			},
		}
		return &DatasetAPI{
			dataStore:              store.DataStore{Backend: mockedDataStore},
			enablePrivateEndpoints: true,
			EnablePrePublishView:   true,
			authMiddleware: &authMock.MiddlewareMock{
				ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
					return testEntityData, nil
				},
			},
			permissionsChecker: permissionsChecker,
		}, permissionsChecker
	}

	newRequest := func(datasetID, edition string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/datasets/"+datasetID+"/editions/"+edition, http.NoBody)
		req.Header.Set(dprequest.AuthHeaderKey, dprequest.BearerPrefix+"valid-token")
		return mux.SetURLVars(req, map[string]string{"dataset_id": datasetID, "edition": edition})
	}

	Convey("Given the series and edition IDs have both changed since the viewer was given access", t, func() {
		Convey("When the request uses the current series ID and the previous edition ID", func() {
			mockedDataStore := newMockedDataStore()
			api, _ := newAPI(mockedDataStore, "old-series/old-edition")

			attributes, err := api.getPermissionAttributesFromRequest(newRequest("new-series", "old-edition"))

			Convey("Then it returns the permitted previous series and edition", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": "old-series/old-edition"})
			})

			Convey("And the current edition is found from the previous edition ID", func() {
				So(mockedDataStore.CheckEditionExistsStaticCalls()[0].EditionID, ShouldEqual, "old-edition")
				So(mockedDataStore.GetVersionsStaticByPreviousEditionIDCalls()[0].PreviousEditionID, ShouldEqual, "old-edition")
				So(mockedDataStore.GetVersionsStaticByEditionNoLimitCalls()[0].Edition, ShouldEqual, "new-edition")
			})
		})

		Convey("When the request uses the previous series ID and the previous edition ID", func() {
			mockedDataStore := newMockedDataStore()
			api, _ := newAPI(mockedDataStore, "old-series/old-edition")

			attributes, err := api.getPermissionAttributesFromRequest(newRequest("old-series", "old-edition"))

			Convey("Then it returns the permitted previous series and edition", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": "old-series/old-edition"})
			})

			Convey("And the dataset is found from the previous series ID", func() {
				So(mockedDataStore.GetDatasetByPreviousSeriesIDCalls()[0].PreviousSeriesID, ShouldEqual, "old-series")
				So(mockedDataStore.GetVersionsStaticByEditionNoLimitCalls()[0].DatasetID, ShouldEqual, "new-series")
			})
		})

		Convey("When the viewer does not have access to any current or previous series and edition", func() {
			mockedDataStore := newMockedDataStore()
			api, permissionsChecker := newAPI(mockedDataStore, "unrelated-series/unrelated-edition")

			attributes, err := api.getPermissionAttributesFromRequest(newRequest("new-series", "old-edition"))

			Convey("Then it falls back to the series and edition from the request", func() {
				So(err, ShouldBeNil)
				So(attributes, ShouldResemble, map[string]string{"dataset_edition": "new-series/old-edition"})
				So(len(permissionsChecker.HasPermissionCalls()), ShouldBeGreaterThan, 0)
			})
		})
	})
}
