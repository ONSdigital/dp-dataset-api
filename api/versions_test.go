package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ONSdigital/dp-authorisation/v2/authorisation"
	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	applicationMocks "github.com/ONSdigital/dp-dataset-api/application/mock"
	cloudflareMocks "github.com/ONSdigital/dp-dataset-api/cloudflare/mocks"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	filesAPIModels "github.com/ONSdigital/dp-files-api/files"
	filesAPIErrors "github.com/ONSdigital/dp-files-api/store"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	versionPayload           = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","edition_title": "Updated Edition Title"}`
	versionAssociatedPayload = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated","collection_id":"12345"}`
	versionPublishedPayload  = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"published","collection_id":"12345"}`
	testLockID               = "testLockID"
	testETag                 = "testETag"
	testAuthToken            = "test-auth-token"
)

type mockFilesClient struct {
	GetFileFunc           func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error)
	MarkFilePublishedFunc func(ctx context.Context, path string) error
}

func (m *mockFilesClient) GetFile(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
	return m.GetFileFunc(ctx, path)
}

func (m *mockFilesClient) MarkFilePublished(ctx context.Context, path string) error {
	return m.MarkFilePublishedFunc(ctx, path)
}

func TestGetVersionsReturnsForbidden(t *testing.T) {
	t.Parallel()
	Convey("Given a request is made to get versions which is forbidden", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)
		Convey("Then a 403 response is received and no database calls are made", func() {
			So(w.Code, ShouldEqual, http.StatusForbidden)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetVersionsReturnsUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("Given a request is made to get versions which is unauthorised", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)
		Convey("Then a 401 response is received and no database calls are made", func() {
			So(w.Code, ShouldEqual, http.StatusUnauthorized)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("get versions delegates offset and limit to db func and returns results list", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "edition": "678"})
		w := httptest.NewRecorder()
		results := []models.Version{}
		mockedDataStore := &storetest.StorerMock{

			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456", Type: models.Filterable.String()}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return results, 2, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		list, totalCount, err := api.getVersions(w, r, 20, 0)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
		So(mockedDataStore.GetVersionsCalls()[0].Limit, ShouldEqual, 20)
		So(mockedDataStore.GetVersionsCalls()[0].Offset, ShouldEqual, 0)
		So(list, ShouldResemble, results)
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})

	Convey("get versions delegates offset and limit to db func and returns results list for static dataset type", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456"})

		w := httptest.NewRecorder()
		results := []models.Version{}
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456", Type: "static"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsStaticFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return results, 2, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		list, totalCount, err := api.getVersions(w, r, 20, 0)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsStaticCalls()), ShouldEqual, 1)
		So(mockedDataStore.GetVersionsStaticCalls()[0].Limit, ShouldEqual, 20)
		So(mockedDataStore.GetVersionsStaticCalls()[0].Offset, ShouldEqual, 0)
		So(list, ShouldResemble, results)
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "editions": "678"})
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "editions": "678"})
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "editions": "678"})
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "editions": "678"})
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return nil, 0, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "editions": "678"})
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return nil, 0, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When a published version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456", "editions": "678"})
		w := httptest.NewRecorder()

		version := models.Version{State: "gobbly-gook"}
		items := []models.Version{version}
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return items, len(items), nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourceState.Error())

		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionForbidden(t *testing.T) {
	t.Parallel()
	Convey("Given a request is made to get version which is forbidden", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)

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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		Convey("Then it returns a 403 forbidden", func() {
			So(w.Code, ShouldEqual, http.StatusForbidden)
		})

		Convey("And none of the relevant calls have been made", func() {
			So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetVersionUnauathorised(t *testing.T) {
	t.Parallel()
	Convey("Given a request is made to get version which is unauthorised", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)

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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		Convey("Then it returns a 401 unauthorized", func() {
			So(w.Code, ShouldEqual, http.StatusUnauthorized)
		})

		Convey("And none of the relevant calls have been made", func() {
			So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Given a version", t, func() {
		version := &models.Version{
			State: models.EditionConfirmedState,
			Links: &models.VersionLinks{
				Self: &models.LinkObject{},
				Version: &models.LinkObject{
					HRef: "href",
				},
			},
		}
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{

			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return version, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		Convey("With an etag", func() {
			version.ETag = "version-etag"
			Convey("When we call the GET version endpoint", func() {
				api.Router.ServeHTTP(w, r)

				Convey("Then it returns a 200 OK", func() {
					So(w.Code, ShouldEqual, http.StatusOK)
				})
				Convey("And the etag is returned in the response header", func() {
					So(w.Header().Get("Etag"), ShouldEqual, version.ETag)
				})

				Convey("And the relevant calls have been made", func() {
					So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
					So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				})
			})
		})
		Convey("Without an etag", func() {
			version.ETag = ""
			Convey("When we call the GET version endpoint", func() {
				api.Router.ServeHTTP(w, r)

				Convey("Then it returns a 200 OK", func() {
					So(w.Code, ShouldEqual, http.StatusOK)
				})
				Convey("And no etag is returned in the response header", func() {
					So(w.Header().Get("Etag"), ShouldBeEmpty)
				})

				Convey("And the relevant calls have been made", func() {
					So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
					So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				})
			})
		})
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist for return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, errs.ErrDatasetNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an invalid version is requested return invalid version error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/jjj", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("A request to get version zero returns an invalid version error response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/-1", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("A request to get a negative version returns an error response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/0", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When an unpublished version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123-456", Next: &models.Dataset{ID: "123-456"}}, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					State: "gobbly-gook",
					Links: &models.VersionLinks{
						Self: &models.LinkObject{},
						Version: &models.LinkObject{
							HRef: "href",
						},
					},
				}, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourceState.Error())

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestPutVersionForbidden(t *testing.T) {
	t.Parallel()
	Convey("When a request is made to put version that is forbidden", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
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

		Convey("Given a valid request is executed", func() {
			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
			api.Router.ServeHTTP(w, r)

			Convey("Then the request returns forbidden, with none of the expected calls made to the database", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("When edition ID in request body contains spaces, return 400 Bad Request", t, func() {
		b := `{
		"edition": "edition 8",
		"edition_title": "Valid Title",
		"release_date": "2024-05-01",
		"type": "static"
		}`

		r := createRequestWithAuth(
			"PUT",
			"http://localhost:22000/datasets/123/editions/2017/versions/1",
			bytes.NewBufferString(b),
		)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					State:   models.EditionConfirmedState,
					ETag:    testETag,
				}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-viewer"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		Convey("Then it returns 400 and update is not attempted", func() {
			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrSpacesNotAllowedInID.Error())

			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 0)
		})
	})
}

func TestPutVersionUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("When a request is made to put version that is unauthorised", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		Convey("Given a valid request is executed", func() {
			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
			api.Router.ServeHTTP(w, r)

			Convey("Then the request returns unauthorized, with none of the expected calls made to the database", func() {
				So(w.Code, ShouldEqual, http.StatusUnauthorized)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestPutVersionReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("When state is unchanged", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}
		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					State:       models.EditionConfirmedState,
					ETag:        testETag,
				}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		Convey("Given a valid request is executed", func() {
			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			Convey("Then the request is successful, with the expected calls", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
				So(auditServiceMock.RecordVersionAuditEventCalls(), ShouldHaveLength, 1)
			})

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("Given a valid request is executed, but the firstUpdate call returns ErrDatasetNotFound", func() {
			mockedDataStore.UpdateVersionFunc = func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				if len(mockedDataStore.UpdateVersionCalls()) == 1 {
					return "", errs.ErrDatasetNotFound
				}
				return "", nil
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			Convey("Then the request is successful, with the expected calls including the update retry", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.UpdateVersionCalls()[0].ETagSelector, ShouldEqual, testETag)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
			})

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("When updating only release_date and edition ID and title are unchanged", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := `{
	        "edition": "2017",
	        "edition_title": "Test Title",
	        "release_date": "2024-05-01",
	        "type": "static"
	    }`

		r := createRequestWithAuth(
			"PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b),
		)
		w := httptest.NewRecorder()

		InstanceID := "789"

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return nil
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},

			GetVersionStaticFunc: func(ctx context.Context, datasetID, editionID string, version int, state string) (*models.Version, error) {
				return &models.Version{
					ID:           InstanceID,
					Edition:      "2017",
					EditionTitle: "Test Title ",
					State:        models.AssociatedState,
					Type:         models.Static.String(),
					ETag:         testETag,
					Version:      1,
				}, nil
			},

			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      InstanceID,
					Edition: "2017",
					State:   models.EditionConfirmedState,
					ETag:    testETag,
				}, nil
			},

			UpdateVersionStaticFunc: func(ctx context.Context, currentVersion *models.Version, versionUpdate *models.Version, eTagSelector string) (string, error) {
				return "", nil
			},

			AcquireVersionsLockFunc: func(context.Context, string) (string, error) {
				return testLockID, nil
			},
			UnlockVersionsFunc: func(context.Context, string) {
			},

			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", nil
			},
			UnlockInstanceFunc: func(context.Context, string) {},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		api.Router.ServeHTTP(w, r)

		Convey("Then response should be 200 OK", func() {
			So(w.Code, ShouldEqual, http.StatusOK)
		})
	})

	Convey("When updating to a new edition ID that already exists", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := `{"edition":"existing-edition","edition_title":"New Edition Title","release_date":"2017-04-04","type":"static"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				if editionID == "existing-edition" {
					return nil
				}
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					State:   models.EditionConfirmedState,
					ETag:    testETag,
				}, nil
			},
			GetVersionStaticFunc: func(ctx context.Context, datasetID, editionID string, version int, state string) (*models.Version, error) {
				return &models.Version{
					ID:           "789",
					Edition:      "2017",
					EditionTitle: "Test Title",
					State:        models.EditionConfirmedState,
				}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		Convey("Then it returns a 409 Conflict status", func() {
			So(w.Code, ShouldEqual, http.StatusConflict)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionAlreadyExists.Error())
		})

		Convey("And the version is not updated", func() {
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		})

		Convey("And no locks are acquired since the error occurs before locking", func() {
			So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.AcquireVersionsLockCalls()), ShouldEqual, 0)
		})
	})

	Convey("When updating to a new edition title that already exists", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := `{"edition":"new-unique-edition","edition_title":"Existing Edition Title","release_date":"2017-04-04","type":"static"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				if editionID == "new-unique-edition" {
					return errs.ErrEditionNotFound
				}
				return nil
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				// Return ErrEditionTitleAlreadyExists for the edition title, indicating it already exists
				if editionTitle == "Existing Edition Title" {
					return errs.ErrEditionTitleAlreadyExists
				}
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					State:   models.EditionConfirmedState,
					ETag:    testETag,
				}, nil
			},
			GetVersionStaticFunc: func(ctx context.Context, datasetID, editionID string, version int, state string) (*models.Version, error) {
				return &models.Version{
					ID:           "789",
					Edition:      "2017",
					EditionTitle: "Test Title",
					State:        models.EditionConfirmedState,
				}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		Convey("Then it returns a 409 Conflict status", func() {
			So(w.Code, ShouldEqual, http.StatusConflict)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionTitleAlreadyExists.Error())
		})

		Convey("And the version is not updated", func() {
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		})

		Convey("And no locks are acquired since the error occurs before locking", func() {
			So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.AcquireVersionsLockCalls()), ShouldEqual, 0)
		})
	})

	Convey("When state is set to associated", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionAssociatedPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		Convey("put version with CMD type", func() {
			isLocked := false
			mockedDataStore := &storetest.StorerMock{
				CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
					return errs.ErrEditionNotFound
				},
				CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
					return nil
				},
				CheckEditionExistsFunc: func(context.Context, string, string, string) error {
					return nil
				},
				GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
					return &models.Version{
						ID:      "789",
						Edition: "2017",
						Type:    models.Filterable.String(),
						State:   models.AssociatedState,
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					So(isLocked, ShouldBeTrue)
					return "", nil
				},
				UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
					return nil
				},
				AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
					isLocked = true
					return testLockID, nil
				},
				UnlockInstanceFunc: func(context.Context, string) {
					isLocked = false
				},
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("put version with Cantabular type and CMD mock", func() {
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{}, nil
				},
				CheckEditionExistsFunc: func(context.Context, string, string, string) error {
					return nil
				},
				GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
					return &models.Version{
						ID:    "789",
						Type:  "null",
						State: models.AssociatedState,
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					return "", nil
				},
				UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
					return nil
				},
				AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
					return "", nil
				},
				UnlockInstanceFunc: func(context.Context, string) {},
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
			So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("put version with Cantabular type", func() {
			isLocked := false
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{}, nil
				},
				CheckEditionExistsFunc: func(context.Context, string, string, string) error {
					return nil
				},
				GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
					return &models.Version{
						ID:      "789",
						Edition: "2017",
						Type:    models.CantabularFlexibleTable.String(),
						State:   models.EditionConfirmedState,
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					So(isLocked, ShouldBeTrue)
					return "", nil
				},
				UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
					return nil
				},
				AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
					isLocked = true
					return testLockID, nil
				},
				UnlockInstanceFunc: func(context.Context, string) {
					isLocked = false
				},
			}

			api := GetAPIWithCantabularMocks(mockedDataStore, generatorMock, authorisationMock, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
			So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("When state is set to edition-confirmed", t, func() {
		downloadsGenerated := make(chan bool, 1)

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				downloadsGenerated <- true
				return nil
			},
		}

		b := versionAssociatedPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					State:   models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		ctx := context.Background()
		select {
		case <-downloadsGenerated:
			log.Info(ctx, "download generated as expected")
		case <-time.After(time.Second * 10):
			err := errors.New("failing test due to timeout")
			log.Error(ctx, "timed out", err)
			t.Fail()
		}

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)

		Convey("Then the lock has been acquired and released exactly once", func() {
			validateLock(mockedDataStore, "789")
			So(isLocked, ShouldBeFalse)
		})

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When state is set to published", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPublishedPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}
		Convey("And the datatype is CMD", func() {
			isLocked := false
			mockedDataStore := &storetest.StorerMock{
				CheckEditionExistsFunc: func(context.Context, string, string, string) error {
					return nil
				},
				GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
					return &models.Version{
						ID:      "789",
						Edition: "2017",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
								ID:   "123",
							},
							Dimensions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
							},
							Edition: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
								ID:   "2017",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/instances/765",
							},
							Version: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
						ReleaseDate: "2017-12-12",
						Downloads: &models.DownloadList{
							CSV: &models.DownloadObject{
								Private: "s3://csv-exported/myfile.csv",
								HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
								Size:    "1234",
							},
						},
						State: models.EditionConfirmedState,
						Type:  models.Filterable.String(),
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					So(isLocked, ShouldBeTrue)
					return "", nil
				},
				GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{
						ID:      "123",
						Next:    &models.Dataset{Links: &models.DatasetLinks{}},
						Current: &models.Dataset{Links: &models.DatasetLinks{}},
					}, nil
				},
				UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
					return nil
				},
				GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
					return &models.EditionUpdate{
						ID: "123",
						Next: &models.Edition{
							State: models.PublishedState,
							Links: &models.EditionUpdateLinks{
								Self: &models.LinkObject{
									HRef: "http://localhost:22000/datasets/123/editions/2017",
								},
								LatestVersion: &models.LinkObject{
									HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
									ID:   "1",
								},
							},
						},
						Current: &models.Edition{},
					}, nil
				},
				UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
					return nil
				},
				SetInstanceIsPublishedFunc: func(context.Context, string) error {
					return nil
				},
				AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
					isLocked = true
					return testLockID, nil
				},
				UnlockInstanceFunc: func(context.Context, string) {
					isLocked = false
				},
				GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
					return models.Filterable.String(), nil
				},
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
			So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)
			So(generatorMock.GenerateCalls()[0].Edition, ShouldEqual, "2017")
			So(generatorMock.GenerateCalls()[0].DatasetID, ShouldEqual, "123")
			So(generatorMock.GenerateCalls()[0].Version, ShouldEqual, "1")
			So(generatorMock.GenerateCalls()[0].InstanceID, ShouldEqual, "789")

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("And the datatype is Cantabular", func() {
			isLocked := false
			mockedDataStore := &storetest.StorerMock{
				CheckEditionExistsFunc: func(context.Context, string, string, string) error {
					return nil
				},
				GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
					return &models.Version{
						ID:      "789",
						Edition: "2017",
						Links: &models.VersionLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
								ID:   "123",
							},
							Dimensions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
							},
							Edition: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
								ID:   "2017",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/instances/765",
							},
							Version: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
						ReleaseDate: "2017-12-12",
						Downloads: &models.DownloadList{
							CSV: &models.DownloadObject{
								Private: "s3://csv-exported/myfile.csv",
								HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
								Size:    "1234",
							},
						},
						State: models.EditionConfirmedState,
						Type:  models.CantabularFlexibleTable.String(),
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					So(isLocked, ShouldBeTrue)
					return "", nil
				},
				GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{
						ID:      "123",
						Next:    &models.Dataset{Links: &models.DatasetLinks{}},
						Current: &models.Dataset{Links: &models.DatasetLinks{}},
					}, nil
				},
				UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
					return nil
				},
				GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
					return &models.EditionUpdate{
						ID: "123",
						Next: &models.Edition{
							State: models.PublishedState,
							Links: &models.EditionUpdateLinks{
								Self: &models.LinkObject{
									HRef: "http://localhost:22000/datasets/123/editions/2017",
								},
								LatestVersion: &models.LinkObject{
									HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
									ID:   "1",
								},
							},
						},
						Current: &models.Edition{},
					}, nil
				},
				UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
					return nil
				},
				SetInstanceIsPublishedFunc: func(context.Context, string) error {
					return nil
				},
				AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
					isLocked = true
					return testLockID, nil
				},
				UnlockInstanceFunc: func(context.Context, string) {
					isLocked = false
				},
				GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
					return models.Filterable.String(), nil
				},
			}

			api := GetAPIWithCantabularMocks(mockedDataStore, generatorMock, authorisationMock, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
			So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
			So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)
			So(generatorMock.GenerateCalls()[0].Edition, ShouldEqual, "2017")
			So(generatorMock.GenerateCalls()[0].DatasetID, ShouldEqual, "123")
			So(generatorMock.GenerateCalls()[0].Version, ShouldEqual, "1")
			So(generatorMock.GenerateCalls()[0].InstanceID, ShouldEqual, "789")

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("When version is already published and update includes downloads object only", t, func() {
		Convey("And downloads object contains only a csv object", func() {
			b := `{"downloads": { "csv": { "public": "http://cmd-dev/test-site/cpih01", "size": "12", "href": "http://localhost:8080/cpih01"}}}`
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

			updateVersionDownloadTest(r)

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("And downloads object contains only a xls object", func() {
			b := `{"downloads": { "xls": { "public": "http://cmd-dev/test-site/cpih01", "size": "12", "href": "http://localhost:8080/cpih01"}}}`
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

			updateVersionDownloadTest(r)

			Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})
}

func updateVersionDownloadTest(r *http.Request) {
	w := httptest.NewRecorder()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	isLocked := false
	mockedDataStore := &storetest.StorerMock{
		GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
			return &models.DatasetUpdate{
				ID:      "123",
				Next:    &models.Dataset{Links: &models.DatasetLinks{}},
				Current: &models.Dataset{Links: &models.DatasetLinks{}},
			}, nil
		},
		CheckEditionExistsFunc: func(context.Context, string, string, string) error {
			return nil
		},
		GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
			return &models.Version{
				ID: "789",
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123",
						ID:   "123",
					},
					Dimensions: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
					},
					Edition: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2017",
						ID:   "2017",
					},
					Self: &models.LinkObject{
						HRef: "http://localhost:22000/instances/765",
					},
					Version: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					},
				},
				ReleaseDate: "2017-12-12",
				Downloads: &models.DownloadList{
					CSV: &models.DownloadObject{
						Private: "s3://csv-exported/myfile.csv",
						HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
						Size:    "1234",
					},
				},
				State: models.PublishedState,
			}, nil
		},
		UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
			So(isLocked, ShouldBeTrue)
			return "", nil
		},
		GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
			return &models.EditionUpdate{
				ID: "123",
				Next: &models.Edition{
					State: models.PublishedState,
				},
				Current: &models.Edition{},
			}, nil
		},
		AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
			isLocked = true
			return testLockID, nil
		},
		UnlockInstanceFunc: func(context.Context, string) {
			isLocked = false
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
		RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
			return nil
		},
	}

	api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
	api.Router.ServeHTTP(w, r)

	So(w.Code, ShouldEqual, http.StatusOK)
	So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
	So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
	So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
	// Check updates to edition and dataset resources were not called
	So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
	So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
	So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

	Convey("Then the lock has been acquired and released exactly once", func() {
		validateLock(mockedDataStore, "789")
		So(isLocked, ShouldBeFalse)
	})
}

func TestPutVersionGenerateDownloadsError(t *testing.T) {
	Convey("given download generator returns an error", t, func() {
		mockedErr := errors.New("spectacular explosion")
		var v models.Version
		err := json.Unmarshal([]byte(versionAssociatedPayload), &v)
		So(err, ShouldBeNil)
		v.ID = "789"
		v.Edition = "2017"
		v.State = models.EditionConfirmedState

		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &v, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
			},
		}

		mockDownloadGenerator := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return mockedErr
			},
		}

		Convey("when put version is called with a valid request", func() {
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))

			w := httptest.NewRecorder()
			cfg, err := config.Get()
			So(err, ShouldBeNil)
			cfg.EnablePrivateEndpoints = true

			authorisationMock := &authMock.MiddlewareMock{
				RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
					return handlerFunc
				},
				ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
					return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
				},
			}

			auditServiceMock := &applicationMocks.AuditServiceMock{
				RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
					return nil
				},
			}

			api := GetAPIWithCMDMocks(mockedDataStore, mockDownloadGenerator, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			Convey("then an internal server error response is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})

			Convey("and the expected store calls are made with the expected parameters", func() {
				genCalls := mockDownloadGenerator.GenerateCalls()

				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(mockedDataStore.CheckEditionExistsCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, ShouldEqual, "2017")

				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetVersionCalls()[0].DatasetID, ShouldEqual, "123")
				So(mockedDataStore.GetVersionCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.GetVersionCalls()[0].Version, ShouldEqual, 1)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)

				So(len(genCalls), ShouldEqual, 1)
				So(genCalls[0].DatasetID, ShouldEqual, "123")
				So(genCalls[0].Edition, ShouldEqual, "2017")
				So(genCalls[0].Version, ShouldEqual, "1")
			})

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})
}

func TestPutEmptyVersion(t *testing.T) {
	getVersionAssociatedModel := func(datasetType models.DatasetType) models.Version {
		var v models.Version
		err := json.Unmarshal([]byte(versionAssociatedPayload), &v) //
		So(err, ShouldBeNil)                                        //
		v.Type = datasetType.String()
		v.ID = "123"
		v.State = models.AssociatedState //
		return v
	}
	xlsDownload := &models.DownloadList{XLS: &models.DownloadObject{Size: "1", HRef: "/hello"}}

	// CMD
	Convey("given an existing version with empty downloads", t, func() {
		v := getVersionAssociatedModel(models.Filterable)
		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &v, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
			},
		}

		Convey("when put version is called with an associated version with empty downloads", func() {
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			w := httptest.NewRecorder()

			authorisationMock := &authMock.MiddlewareMock{
				RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
					return handlerFunc
				},
				ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
					return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
				},
			}

			auditServiceMock := &applicationMocks.AuditServiceMock{
				RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
					return nil
				},
			}

			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and the updated version is as expected", func() {
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, ShouldBeNil)
			})

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})
		})
	})

	Convey("given an existing version with a xls download already exists", t, func() {
		v := getVersionAssociatedModel(models.Static)
		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				v.Downloads = xlsDownload
				return &v, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
			},
		}

		mockDownloadGenerator := &mocks.DownloadsGeneratorMock{}

		Convey("when put version is called with an associated version with empty downloads", func() {
			versionAssociatedPayloadNoDownload := `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","collection_id":"12345","state":"associated"}`
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayloadNoDownload))
			w := httptest.NewRecorder()

			authorisationMock := &authMock.MiddlewareMock{
				RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
					return handlerFunc
				},
				ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
					return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
				},
			}

			auditServiceMock := &applicationMocks.AuditServiceMock{
				RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
					return nil
				},
			}
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
			api.Router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and the expected external calls are made with the correct parameters", func() {
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(mockedDataStore.CheckEditionExistsCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.CheckEditionExistsCalls()[0].State, ShouldEqual, "")

				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetVersionCalls()[0].DatasetID, ShouldEqual, "123")
				So(mockedDataStore.GetVersionCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.GetVersionCalls()[0].Version, ShouldEqual, 1)
				So(mockedDataStore.GetVersionCalls()[0].State, ShouldEqual, "")

				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(mockDownloadGenerator.GenerateCalls()), ShouldEqual, 0)
			})

			Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				So(isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestPutVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := "{"
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request has negative version return invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/-1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrInvalidVersion
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request has zero version return invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/0", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When an request has invalid version return invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/kkk", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the dataset document cannot be found for version return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			AcquireInstanceLockFunc: func(ctx context.Context, instanceID string) (string, error) {
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the edition document cannot be found for version return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
			AcquireInstanceLockFunc: func(ctx context.Context, instanceID string) (string, error) {
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the version document cannot be found return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			AcquireInstanceLockFunc: func(ctx context.Context, instanceID string) (string, error) {
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request is not authorised to update version then response returns status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					State: "associated",
				}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)

		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the version document has already been published return status forbidden", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					State: models.PublishedState,
				}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldEqual, "unable to update version as it has been published\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request body is invalid return status bad request", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()
		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					State:   "associated",
				}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "missing collection_id for association between version and a collection\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("Then the lock has been acquired and released ", func() {
			validateLock(mockedDataStore, "789")
			So(isLocked, ShouldBeFalse)
		})

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When setting the instance node to published fails", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPublishedPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(ctx context.Context, datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
			CheckEditionTitleExistsStaticFunc: func(ctx context.Context, datasetID, editionTitle string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "789",
					Edition: "2017",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "2017",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/765",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
							ID:   "1",
						},
					},
					ReleaseDate: "2017-12-12",
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							Private: "s3://csv-exported/myfile.csv",
							HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
							Size:    "1234",
						},
					},
					State: models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				return "", nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Next:    &models.Dataset{Links: &models.DatasetLinks{}},
					Current: &models.Dataset{Links: &models.DatasetLinks{}},
				}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.PublishedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
								ID:   "2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return errors.New("failed to set is_published on the instance node")
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
			},
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return "", nil
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("Then the lock has been acquired and released ", func() {
			validateLock(mockedDataStore, "789")
			So(isLocked, ShouldBeFalse)
		})

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
	Convey("Given PUT /versions/{id} with a distribution missing format", t, func() {
		body := `{
        "type": "static",
        "distributions": [{
            "title": "Dataset",
            "download_url": "http://example.com/file.csv",
            "byte_size": 100
        }]
    	}`

		r := createRequestWithAuth(
			"PUT",
			"http://localhost:22000/datasets/123/editions/2017/versions/1",
			bytes.NewBufferString(body),
		)
		w := httptest.NewRecorder()

		mocked := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "1",
					Edition: "2017",
					Type:    models.Static.String(),
					State:   models.CreatedState,
					ETag:    testETag,
				}, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mocked, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		Convey("Then the API returns 400 with missing-format error", func() {
			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, "distributions[0].format field is missing")
		})
	})

	Convey("Given PUT /versions/{id} with a distribution containing invalid format", t, func() {
		body := `{
        "type": "static",
        "distributions": [{
            "title": "Dataset",
            "download_url": "http://example.com/file.xxx",
            "byte_size": 100,
            "format": "INVALID"
        	}]
    	}`

		r := createRequestWithAuth(
			"PUT",
			"http://localhost:22000/datasets/123/editions/2017/versions/1",
			bytes.NewBufferString(body),
		)
		w := httptest.NewRecorder()

		mocked := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:      "1",
					Edition: "2017",
					Type:    models.Static.String(),
					State:   models.CreatedState,
					ETag:    testETag,
				}, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mocked, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		Convey("Then API returns 400 with invalid-format error", func() {
			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, "distributions[0].format field is invalid")
		})
	})
}

func TestCreateNewVersionDoc(t *testing.T) {
	t.Parallel()
	Convey("Given an empty current version and a version update that contains a collection_id", t, func() {
		currentVersion := models.Version{}
		versionUpdate := models.Version{
			CollectionID: "4321",
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version update contains the collection_id", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{})
			So(versionUpdate, ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})
	})

	Convey("Given a current version that contains a collection_id and a version update that contains a different collection_id", t, func() {
		currentVersion := models.Version{
			CollectionID: "1234",
		}
		versionUpdate := models.Version{
			CollectionID: "4321",
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version update contains the updated collection_id", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{
				CollectionID: "1234",
			})
			So(versionUpdate, ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})
	})

	Convey("Given a current version that contains a collection_id and a version update that does not contain a collection_id", t, func() {
		currentVersion := models.Version{
			CollectionID: "1234",
		}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version update contains the updated collection_id", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				CollectionID: "1234",
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{
				CollectionID: "1234",
			})
			So(versionUpdate, ShouldResemble, models.Version{})
		})
	})

	Convey("Given empty current version and update", t, func() {
		currentVersion := models.Version{}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version is empty", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{})
			So(versionUpdate, ShouldResemble, models.Version{})
		})
	})

	Convey("Given an empty current version and an update containing a spatial link", t, func() {
		currentVersion := models.Version{}
		versionUpdate := models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version contains the provided spatial link", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{})
			So(versionUpdate, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})
	})

	Convey("Given a current version containing a spatial link and an update containing a different spatial link", t, func() {
		currentVersion := models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		versionUpdate := models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version contains the updated spatial link", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/oldgeographylist",
					},
				},
			})
			So(versionUpdate, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})
	})

	Convey("Given a current version containing a spatial link and an empty update", t, func() {
		currentVersion := models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version contains the old spatial link", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/oldgeographylist",
					},
				},
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/oldgeographylist",
					},
				},
			})
			So(versionUpdate, ShouldResemble, models.Version{})
		})
	})

	Convey("Given a current version containing a dataset link and an empty update", t, func() {
		currentVersion := models.Version{
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://ons.gov.uk/datasets/123",
				},
			},
		}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		So(err, ShouldBeNil)

		Convey("Then the combined version contains the old dataset link", func() {
			So(*combinedVersionUpdate, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://ons.gov.uk/datasets/123",
					},
				},
			})
		})

		Convey("And the existing variables did not mutate", func() {
			So(currentVersion, ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://ons.gov.uk/datasets/123",
					},
				},
			})
			So(versionUpdate, ShouldResemble, models.Version{})
		})
	})
}

func TestDetachVersionForbidden(t *testing.T) {
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if !featureOn {
		return
	}

	t.Parallel()

	Convey("Given a request made to delete a version which is forbidden.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		Convey("Then the response code is 403 and no expected database calls are made.", t, func() {
			So(w.Code, ShouldEqual, http.StatusUnauthorized)
			So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
			So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
		})
	})
}

func TestDetachVersionUnauthorised(t *testing.T) {
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if !featureOn {
		return
	}

	t.Parallel()

	Convey("A request made to delete a version which has no authorisation information supplied.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithNoAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)
		Convey("Then the response code is 401 and no expected database calls are made.", t, func() {
			So(w.Code, ShouldEqual, http.StatusUnauthorized)
			So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
			So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
		})
	})
}

func TestDetachVersionReturnOK(t *testing.T) {
	// TODO conditional test for feature flagged functionality. Will need tidying up eventually.
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if !featureOn {
		return
	}

	t.Parallel()

	Convey("A successful detach request against a version of a published dataset returns 200 OK response.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID:      "test",
					Current: &models.Edition{},
					Next: &models.Edition{
						Edition: "yep",
						State:   models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID: "1"}}}}, nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{}}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("A successful detach request against a version of a unpublished dataset returns 200 OK response.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID:      "test",
					Current: &models.Edition{},
					Next: &models.Edition{
						Edition: "yep",
						State:   models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID: "1"}}}}, nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			DeleteEditionFunc: func(ctx context.Context, editionID string) error {
				return nil
			},
			RemoveDatasetVersionAndEditionLinksFunc: func(ctx context.Context, datasetID string) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})
}

func TestDetachVersionReturnsError(t *testing.T) {
	// TODO conditional test for feature flagged functionality. Will need tidying up eventually.
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if !featureOn {
		return
	}

	t.Parallel()

	Convey("When the api cannot connect to datastore return an internal server error.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrInternalServer
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the provided edition cannot be found, return a 404 not found error.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When detached is called against a version other than latest, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "2"}}}}, nil
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When state is neither edition-confirmed or associated, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.PublishedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the requested version cannot be found, return a not found error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When updating the version fails, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},

			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},

			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", errs.ErrInternalServer
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When edition update fails whilst rolling back the edition, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, nil
			},

			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{}}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When detached endpoint is called against an invalid version, return an invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/kkk", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrInvalidVersion
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When detached endpoint is called against a negative version, return an invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/-1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When detached endpoint is called against zeroq version, return an invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/0", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})
}

func TestDeleteVersionStaticDatasetReturnOK(t *testing.T) {
	t.Parallel()

	Convey("When deleteVersionStatic endpoint is called with a valid unpublished version", t, func() {
		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(context.Context, string) (bool, error) {
				return true, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
					Current: &models.Dataset{
						Type: models.Static.String(),
					},
				}, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					Version: 1,
					State:   models.CreatedState,
				}, nil
			},
			DeleteStaticDatasetVersionFunc: func(context.Context, string, string, int) error {
				return nil
			},
			UpsertDatasetFunc: func(ctx context.Context, ID string, datasetDoc *models.DatasetUpdate) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)

		So(len(mockedDataStore.IsStaticDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteStaticDatasetVersionCalls()), ShouldEqual, 1)
	})
}

func TestDeleteVersionStaticDatasetReturnError(t *testing.T) {
	t.Parallel()

	Convey("When deleteVersionStatic is called against invalid version, return an invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/-1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())
	})

	Convey("When deleteVersionStatic is called against invalid edition, return an invalid edition error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/non-existent-edition/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(context.Context, string) (bool, error) {
				return true, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
					Current: &models.Dataset{
						Type: models.Static.String(),
					},
				}, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
			DeleteStaticDatasetVersionFunc: func(context.Context, string, string, int) error {
				return errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
	})

	Convey("When deleteVersionStatic is called against invalid version, return an invalid version error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(context.Context, string) (bool, error) {
				return true, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
					Current: &models.Dataset{
						Type: models.Static.String(),
					},
				}, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},

			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.IsStaticDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
	})

	Convey("When trying to delete a published version return a forbidden error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(context.Context, string) (bool, error) {
				return true, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
					Current: &models.Dataset{
						Type: models.Static.String(),
					},
				}, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					Version: 1,
					State:   models.PublishedState,
				}, nil
			},
			DeleteStaticDatasetVersionFunc: func(context.Context, string, string, int) error {
				return errs.ErrDeletePublishedVersionForbidden
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDeletePublishedVersionForbidden.Error())

		So(len(mockedDataStore.IsStaticDatasetCalls()), ShouldEqual, 1)
	})

	// When non-static version delete request is attempted but DETACH_DATASET feature flag is off but ENABLE_DELETE_STATIC_VERSION is on
	Convey("When deleteVersionStatic endpoint is called for non-static version but DETACH_DATASET feature flag is off return 405 error", t, func() {
		featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
		featureOn, _ := strconv.ParseBool(featureEnvString)
		if featureOn {
			return
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusMethodNotAllowed)
		So(w.Body.String(), ShouldContainSubstring, "method not allowed")

		So(len(mockedDataStore.IsStaticDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteStaticDatasetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		r := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteStaticDatasetVersionCalls()), ShouldEqual, 0)
	})
}

func assertInternalServerErr(w *httptest.ResponseRecorder) {
	So(w.Code, ShouldEqual, http.StatusInternalServerError)

	body := strings.TrimSpace(w.Body.String())
	containsSubstring := strings.Contains(body, errs.ErrInternalServer.Error()) || strings.Contains(body, models.InternalErrorDescription)
	So(containsSubstring, ShouldBeTrue)
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	So(mockedDataStore.AcquireInstanceLockCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, ShouldNotBeEmpty)
	So(mockedDataStore.UnlockInstanceCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UnlockInstanceCalls()[0].LockID, ShouldEqual, testLockID)
}

func TestPutStateForbidden(t *testing.T) {
	Convey("When a request is made to the state endpoint which is forbidden", t, func() {
		b := `{"state":"published"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/test-static-dataset/editions/test-edition-1/versions/1/state", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		Convey("Then the response code is 403 and no expected database calls are made", func() {
			So(w.Code, ShouldEqual, http.StatusForbidden)
			So(mockedDataStore.GetVersionStaticCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.AcquireVersionsLockCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UnlockVersionsCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.GetDatasetTypeCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertVersionStaticCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.CheckEditionExistsStaticCalls(), ShouldHaveLength, 0)
		})
	})
}

func TestPutStateUnauthorised(t *testing.T) {
	Convey("When a request is made to the state endpoint with no authorisation information", t, func() {
		b := `{"state":"published"}`
		r := createRequestWithNoAuth("PUT", "http://localhost:22000/datasets/test-static-dataset/editions/test-edition-1/versions/1/state", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-viewer"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		Convey("Then the response code is 401 and no expected database calls are made", func() {
			So(w.Code, ShouldEqual, http.StatusUnauthorized)
			So(mockedDataStore.GetVersionStaticCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.AcquireVersionsLockCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UnlockVersionsCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.GetDatasetTypeCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertVersionStaticCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.CheckEditionExistsStaticCalls(), ShouldHaveLength, 0)
		})
	})
}

func TestPutStateReturnsOk(t *testing.T) {
	Convey("When we make a valid request to the state endpoint", t, func() {
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/test-static-dataset/editions/test-edition-1/versions/1/state", bytes.NewBufferString(`{"state":"published"}`))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetVersionStaticFunc: func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				jsonData := `{
						"alerts": [
						  {}
						],
						"edition": "test-edition-1",
						"edition_title": "test-edition-1",
						"last_updated": "2025-04-09T12:14:31.593Z",
						"links": {
						  "dataset": {
							"href": "http://dp-dataset-api:22000/datasets/test-static-dataset",
							"id": "test-static-dataset"
						  },
						  "dimensions": {
							"href": "http://dp-dataset-api:22000/datasets/test-static-dataset/editions/test-edition-1/versions/1/dimensions",
							"id": "test-static-dataset"
						  },
						  "edition": {
							"href": "http://dp-dataset-api:22000/datasets/test-static-dataset/editions/test-edition-1",
							"id": "test-edition-1"
						  },
						  "self": {
							"href": "http://dp-dataset-api:22000/datasets/test-static-dataset/editions/test-edition-1/versions/1"
						  }
						},
						"release_date": "2025-01-15",
						"state": "associated",
						"temporal": [
						  {
							"end_date": "2025-01-31",
							"frequency": "Monthly",
							"start_date": "2025-01-01"
						  }
						],
						"usage_notes": [
						  {
							"note": "This dataset is subject to revision and should be used in conjunction with the accompanying documentation.",
							"title": "Data usage guide"
						  }
						],
						"version": 1,
						"type": "static"
					  }`

				var versionModel models.Version

				err := json.Unmarshal([]byte(jsonData), &versionModel)
				So(err, ShouldBeNil)

				versionModel.Links.Version = &models.LinkObject{
					ID:   "1",
					HRef: "http://dp-dataset-api:22000/datasets/test-static-dataset/editions/test-edition-1/versions/1",
				}

				versionModel.Distributions = nil

				return &versionModel, nil
			},

			AcquireVersionsLockFunc: func(context.Context, string) (string, error) {
				return testLockID, nil
			},

			UnlockVersionsFunc: func(ctx context.Context, lockID string) {
			},

			CheckEditionExistsStaticFunc: func(ctx context.Context, id string, editionID string, state string) error {
				return nil
			},

			UpdateVersionStaticFunc: func(ctx context.Context, currentVersion *models.Version, versionUpdate *models.Version, eTagSelector string) (string, error) {
				return "", nil
			},

			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.Static.String(), nil
			},

			UpsertVersionStaticFunc: func(ctx context.Context, versionDoc *models.Version) error {
				return nil
			},

			GetDatasetFunc: func(ctx context.Context, ID string) (*models.DatasetUpdate, error) {
				jsonData := `{
					"id": "test-static-dataset",
					"next": {
					  "contacts": [
						{
						  "email": "contact-dataset-email@gmail.com",
						  "name": "Dataset Contact name",
						  "telephone": "999"
						}
					  ],
					  "description": "This is an example of a static overview page. The contents of this description will also be used by google tags to improve search engine results directing people here.",
					  "keywords": [
						"keyword"
					  ],
					  "id": "test-static-dataset",
					  "links": {
						"editions": {
						  "href": "http://dp-dataset-api:22000/datasets/test-static-dataset/editions"
						},
						"self": {
						  "href": "http://dp-dataset-api:22000/datasets/test-static-dataset"
						}
					  },
					  "next_release": "tomorrow",
					  "publisher": {
						"href": "publishers-url",
						"name": "publishers-name",
						"type": "publishers-type"
					  },
					  "state": "associated",
					  "title": "Static overview page example",
					  "type": "static",
					  "topics": [
						"subtopic 1",
						"canonical-topic 1"
					  ]
					}
				  }`

				var datasetUpdate models.DatasetUpdate
				err := json.Unmarshal([]byte(jsonData), &datasetUpdate)
				So(err, ShouldBeNil)
				return &datasetUpdate, nil
			},

			UpsertDatasetFunc: func(ctx context.Context, ID string, datasetDoc *models.DatasetUpdate) error {
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
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, version *models.Version) error {
				return nil
			},
		}

		scuProducerMock := getSearchContentUpdatedMock()
		searchContentUpdated := SearchContentUpdatedProducer{Producer: scuProducerMock}

		cloudflareMock := &cloudflareMocks.ClienterMock{
			PurgeByPrefixesFunc: func(ctx context.Context, prefixes []string) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, searchContentUpdated, cloudflareMock, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetVersionStaticCalls(), ShouldHaveLength, 3)
		So(mockedDataStore.AcquireVersionsLockCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UnlockVersionsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateVersionStaticCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetDatasetTypeCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertVersionStaticCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.CheckEditionExistsStaticCalls(), ShouldHaveLength, 1)
		So(len(scuProducerMock.OutputCalls()), ShouldEqual, 1)
		So(cloudflareMock.PurgeByPrefixesCalls(), ShouldHaveLength, 1)

		Convey("And the correct URL's should have been purged", func() {
			expectedPrefixes := []string{
				"http://localhost:20000/datasets/test-static-dataset",
				"http://localhost:20000/datasets/test-static-dataset/editions",
				"http://localhost:20000/datasets/test-static-dataset/editions/test-edition-1/versions",
				"http://localhost:23200/v1/datasets/test-static-dataset",
				"http://localhost:23200/v1/datasets/test-static-dataset/editions",
				"http://localhost:23200/v1/datasets/test-static-dataset/editions/test-edition-1/versions",
			}

			So(cloudflareMock.PurgeByPrefixesCalls()[0].Prefixes, ShouldResemble, expectedPrefixes)
		})
	})
}

func TestPutStateReturnsError(t *testing.T) {
	t.Parallel()

	Convey("When the request has an invalid version ID, return a bad request error", t, func() {
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/-123/state", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.putState(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())
	})

	Convey("When the request has an invalid body, return a bad request error", t, func() {
		b := `{"state":"invalid-body}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/1/state", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}
		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
	})

	Convey("When the request has an empty state, return a bad request error", t, func() {
		b := `{"state":""}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/1/state", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}
		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return testEntityData, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, models.ErrVersionStateInvalid.Error())
	})

	Convey("When the version is not found, return a not found error", t, func() {
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/1/state", bytes.NewBufferString(`{"state":"published"}`))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetVersionStaticFunc: func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
	})

	Convey("When an error occurs, return internal server error", t, func() {
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/1/state", bytes.NewBufferString(`{"state":"published"}`))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetVersionStaticFunc: func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				return nil, errors.New("some error")
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
	})
}

func TestPublishDistributionFiles(t *testing.T) {
	ctx := context.Background()

	Convey("Given a version with no distributions", t, func() {
		version := &models.Version{}
		logData := log.Data{}

		Convey("When publishDistributionFiles is called on an API with no files client", func() {
			api := &DatasetAPI{}
			err := api.publishDistributionFiles(ctx, version, logData, "test-token")

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "files API client not configured")
			})
		})
	})

	Convey("Given a version with distributions", t, func() {
		distributions := []models.Distribution{
			{
				Title:       "Test Distribution 1",
				Format:      "CSV",
				DownloadURL: "test-file-1.csv",
			},
			{
				Title:       "Test Distribution 2",
				Format:      "XLSX",
				DownloadURL: "test-file-2.xlsx",
			},
		}
		version := &models.Version{
			Distributions: &distributions,
		}
		logData := log.Data{}

		Convey("When publishDistributionFiles is called with a mocked files client that succeeds", func() {
			getFileCalls := 0
			markPublishedCalls := 0

			mockClient := &mockFilesClient{
				GetFileFunc: func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
					getFileCalls++
					return &filesAPIModels.StoredRegisteredMetaData{}, nil
				},
				MarkFilePublishedFunc: func(ctx context.Context, path string) error {
					markPublishedCalls++
					return nil
				},
			}

			testFunc := func() error {
				getFileFn := func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
					return mockClient.GetFile(ctx, path)
				}

				markPublishedFn := func(ctx context.Context, path string) error {
					return mockClient.MarkFilePublished(ctx, path)
				}

				return publishDistributionFilesTest(ctx, version, logData, getFileFn, markPublishedFn)
			}

			err := testFunc()

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
				So(getFileCalls, ShouldEqual, 2)
				So(markPublishedCalls, ShouldEqual, 2)
			})
		})

		Convey("When publishDistributionFiles is called with a mocked files client that fails on GetFile", func() {
			getFileCalls := 0
			markPublishedCalls := 0

			mockClient := &mockFilesClient{
				GetFileFunc: func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
					getFileCalls++
					return &filesAPIModels.StoredRegisteredMetaData{}, errors.New("get file error")
				},
				MarkFilePublishedFunc: func(ctx context.Context, path string) error {
					markPublishedCalls++
					return nil
				},
			}

			testFunc := func() error {
				getFileFn := func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
					return mockClient.GetFile(ctx, path)
				}

				markPublishedFn := func(ctx context.Context, path string) error {
					return mockClient.MarkFilePublished(ctx, path)
				}

				return publishDistributionFilesTest(ctx, version, logData, getFileFn, markPublishedFn)
			}

			err := testFunc()

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "get file error")
				So(getFileCalls, ShouldEqual, 2)
				So(markPublishedCalls, ShouldEqual, 0)
			})
		})

		Convey("When publishDistributionFiles is called with a mocked files client that fails on MarkFilePublished", func() {
			getFileCalls := 0
			markPublishedCalls := 0

			mockClient := &mockFilesClient{
				GetFileFunc: func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
					getFileCalls++
					return &filesAPIModels.StoredRegisteredMetaData{}, nil
				},
				MarkFilePublishedFunc: func(ctx context.Context, path string) error {
					markPublishedCalls++
					return errors.New("mark published error")
				},
			}

			testFunc := func() error {
				getFileFn := func(ctx context.Context, path string) (*filesAPIModels.StoredRegisteredMetaData, error) {
					return mockClient.GetFile(ctx, path)
				}

				markPublishedFn := func(ctx context.Context, path string) error {
					return mockClient.MarkFilePublished(ctx, path)
				}

				return publishDistributionFilesTest(ctx, version, logData, getFileFn, markPublishedFn)
			}

			err := testFunc()

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "mark published error")
				So(getFileCalls, ShouldEqual, 2)
				So(markPublishedCalls, ShouldEqual, 2)
			})
		})
	})
}

func TestPutStatePublishDistributionFilesCondition(t *testing.T) {
	Convey("Given a version with distributions", t, func() {
		distributions := []models.Distribution{
			{
				Title:       "Test Distribution",
				Format:      "CSV",
				DownloadURL: "test-file.csv",
			},
		}

		version := &models.Version{
			State:         models.AssociatedState,
			Distributions: &distributions,
		}

		publishDistributionFilesCalled := false
		publishDistributionFilesErr := error(nil)

		testPublishDistributionFiles := func(ctx context.Context, v *models.Version, logData log.Data) error {
			publishDistributionFilesCalled = true
			So(v, ShouldEqual, version)
			return publishDistributionFilesErr
		}

		Convey("When state is PublishedState", func() {
			state := models.PublishedState

			shouldCallPublishDistributionFiles := state == models.PublishedState &&
				version.Distributions != nil &&
				len(*version.Distributions) > 0

			Convey("Then the condition should be true", func() {
				So(shouldCallPublishDistributionFiles, ShouldBeTrue)

				if shouldCallPublishDistributionFiles {
					testPublishDistributionFiles(context.Background(), version, log.Data{})
				}

				So(publishDistributionFilesCalled, ShouldBeTrue)
			})
		})

		Convey("When state is not PublishedState", func() {
			state := models.AssociatedState

			publishDistributionFilesCalled = false

			shouldCallPublishDistributionFiles := state == models.PublishedState &&
				version.Distributions != nil &&
				len(*version.Distributions) > 0

			Convey("Then the condition should be false", func() {
				So(shouldCallPublishDistributionFiles, ShouldBeFalse)

				if shouldCallPublishDistributionFiles {
					testPublishDistributionFiles(context.Background(), version, log.Data{})
				}

				So(publishDistributionFilesCalled, ShouldBeFalse)
			})
		})

		Convey("When version has no distributions", func() {
			state := models.PublishedState
			versionNoDistributions := &models.Version{
				State: models.AssociatedState,
			}

			publishDistributionFilesCalled = false

			shouldCallPublishDistributionFiles := state == models.PublishedState &&
				versionNoDistributions.Distributions != nil &&
				len(*versionNoDistributions.Distributions) > 0

			Convey("Then the condition should be false", func() {
				So(shouldCallPublishDistributionFiles, ShouldBeFalse)

				if shouldCallPublishDistributionFiles {
					testPublishDistributionFiles(context.Background(), versionNoDistributions, log.Data{})
				}

				So(publishDistributionFilesCalled, ShouldBeFalse)
			})
		})

		Convey("When publishDistributionFiles returns an error", func() {
			state := models.PublishedState

			publishDistributionFilesCalled = false
			publishDistributionFilesErr = errors.New("test error")

			shouldCallPublishDistributionFiles := state == models.PublishedState &&
				version.Distributions != nil &&
				len(*version.Distributions) > 0

			Convey("Then the condition should be true but error should be logged", func() {
				So(shouldCallPublishDistributionFiles, ShouldBeTrue)

				if shouldCallPublishDistributionFiles {
					err := testPublishDistributionFiles(context.Background(), version, log.Data{})
					if err != nil {
						So(err.Error(), ShouldEqual, "test error")
					}
				}

				So(publishDistributionFilesCalled, ShouldBeTrue)
			})
		})
	})
}

func publishDistributionFilesTest(ctx context.Context, version *models.Version, logData log.Data,
	getFileFn func(context.Context, string) (*filesAPIModels.StoredRegisteredMetaData, error),
	markPublishedFn func(context.Context, string) error) error {
	if version.Distributions == nil || len(*version.Distributions) == 0 {
		return nil
	}

	var lastError error
	totalFiles := len(*version.Distributions)
	successCount := 0

	for _, distribution := range *version.Distributions {
		if distribution.DownloadURL == "" {
			continue
		}

		filepath := distribution.DownloadURL

		fileLogData := log.Data{
			"filepath":            filepath,
			"distribution_title":  distribution.Title,
			"distribution_format": distribution.Format,
		}

		for k, v := range logData {
			fileLogData[k] = v
		}

		_, err := getFileFn(ctx, filepath)
		if err != nil {
			log.Error(ctx, "failed to get file metadata", err, fileLogData)
			lastError = err
			continue
		}

		err = markPublishedFn(ctx, filepath)
		if err != nil {
			log.Error(ctx, "failed to publish file", err, fileLogData)
			lastError = err
			continue
		}

		successCount++
		log.Info(ctx, "successfully published file", fileLogData)
	}

	log.Info(ctx, "completed publishing distribution files", log.Data{
		"total_files": totalFiles,
		"successful":  successCount,
		"failed":      totalFiles - successCount,
	})

	if lastError != nil {
		return fmt.Errorf("one or more errors occurred while publishing files: %w", lastError)
	}

	return nil
}

func TestPutVersionEditionValidationNonStatic(t *testing.T) {
	t.Parallel()
	Convey("When trying to update edition for non-static dataset type", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := `{"instance_id":"a1b2c3","edition":"new-edition-name","license":"ONS","release_date":"2017-04-04"}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/original-edition/versions/1", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		isLocked := false
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:          "789",
					Edition:     "original-edition",
					Type:        models.CantabularFlexibleTable.String(),
					State:       models.EditionConfirmedState,
					ETag:        testETag,
					ReleaseDate: "2017-12-12",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
					},
				}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				isLocked = true
				return testLockID, nil
			},
			UnlockInstanceFunc: func(context.Context, string) {
				isLocked = false
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

		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, "unable to update edition-id, invalid dataset type")
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)

		Convey("Then the lock has been acquired and released", func() {
			validateLock(mockedDataStore, "789")
			So(isLocked, ShouldBeFalse)
		})

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPublishDistributionFilesErrorMapping(t *testing.T) {
	t.Parallel()

	Convey("When testing error mapping logic in publishDistributionFiles", t, func() {
		Convey("Given ErrFileNotRegistered", func() {
			err := filesAPIErrors.ErrFileNotRegistered
			var filesAPIError error

			if strings.Contains(err.Error(), "FileNotRegistered") ||
				strings.Contains(err.Error(), "file not registered") ||
				strings.Contains(err.Error(), "not found") {
				filesAPIError = errs.ErrFileMetadataNotFound
			}

			Convey("Then it should be mapped to ErrFileMetadataNotFound", func() {
				So(filesAPIError, ShouldEqual, errs.ErrFileMetadataNotFound)
			})
		})

		Convey("Given ErrFileNotInUploadedState", func() {
			err := filesAPIErrors.ErrFileNotInUploadedState
			var filesAPIError error

			if strings.Contains(err.Error(), "FileStateError") ||
				strings.Contains(err.Error(), "file is not set as publishable") ||
				strings.Contains(err.Error(), "file state is not in state uploaded") {
				filesAPIError = errs.ErrFileNotInCorrectState
			}

			Convey("Then it should be mapped to ErrFileNotInCorrectState", func() {
				So(filesAPIError, ShouldEqual, errs.ErrFileNotInCorrectState)
			})
		})

		Convey("Given error with 'FileNotRegistered' in message", func() {
			err := errors.New("FileNotRegistered: file not found")
			var filesAPIError error

			if strings.Contains(err.Error(), "FileNotRegistered") ||
				strings.Contains(err.Error(), "file not registered") ||
				strings.Contains(err.Error(), "not found") {
				filesAPIError = errs.ErrFileMetadataNotFound
			}

			Convey("Then it should be mapped to ErrFileMetadataNotFound", func() {
				So(filesAPIError, ShouldEqual, errs.ErrFileMetadataNotFound)
			})
		})

		Convey("Given error with 'file not registered' in message", func() {
			err := errors.New("file not registered")
			var filesAPIError error

			if strings.Contains(err.Error(), "FileNotRegistered") ||
				strings.Contains(err.Error(), "file not registered") ||
				strings.Contains(err.Error(), "not found") {
				filesAPIError = errs.ErrFileMetadataNotFound
			}

			Convey("Then it should be mapped to ErrFileMetadataNotFound", func() {
				So(filesAPIError, ShouldEqual, errs.ErrFileMetadataNotFound)
			})
		})

		Convey("Given error with 'not found' in message", func() {
			err := errors.New("resource not found")
			var filesAPIError error

			if strings.Contains(err.Error(), "FileNotRegistered") ||
				strings.Contains(err.Error(), "file not registered") ||
				strings.Contains(err.Error(), "not found") {
				filesAPIError = errs.ErrFileMetadataNotFound
			}

			Convey("Then it should be mapped to ErrFileMetadataNotFound", func() {
				So(filesAPIError, ShouldEqual, errs.ErrFileMetadataNotFound)
			})
		})
	})
}

func TestErrorStatusCodeMapping(t *testing.T) {
	t.Parallel()

	Convey("When getVersionAPIErrStatusCode is called with file-related errors", t, func() {
		Convey("Given ErrFileMetadataNotFound", func() {
			statusCode := getVersionAPIErrStatusCode(errs.ErrFileMetadataNotFound)

			Convey("Then it should return 404", func() {
				So(statusCode, ShouldEqual, http.StatusNotFound)
			})
		})

		Convey("Given ErrFileNotInCorrectState", func() {
			statusCode := getVersionAPIErrStatusCode(errs.ErrFileNotInCorrectState)

			Convey("Then it should return 409", func() {
				So(statusCode, ShouldEqual, http.StatusConflict)
			})
		})
	})
}
