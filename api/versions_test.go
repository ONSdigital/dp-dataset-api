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

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	filesAPIModels "github.com/ONSdigital/dp-files-api/files"
	filesAPIErrors "github.com/ONSdigital/dp-files-api/store"
	"github.com/ONSdigital/log.go/v2/log"
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

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("get versions delegates offset and limit to db func and returns results list", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		results := []models.Version{}
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
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return results, 2, nil
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		list, totalCount, err := api.getVersions(w, r, 20, 0)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		list, totalCount, err := api.getVersions(w, r, 20, 0)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
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
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrInternalServer
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrInternalServer
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		assertInternalServerErr(w)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r.Header.Add("internal_token", "coffee")
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		So(err, ShouldNotBeNil)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When a published version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourceState.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

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
					So(datasetPermissions.Required.Calls, ShouldEqual, 1)
					So(permissions.Required.Calls, ShouldEqual, 0)
					So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
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
					So(datasetPermissions.Required.Calls, ShouldEqual, 1)
					So(permissions.Required.Calls, ShouldEqual, 0)
					So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
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
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
	})

	Convey("When the dataset does not exist for return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
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
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
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
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an invalid version is requested return invalid version error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/jjj", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("A request to get version zero returns an invalid version error response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/-1", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("A request to get a negative version returns an error response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/0", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When an unpublished version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourceState.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		Convey("Given a valid request is executed", func() {
			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			Convey("Then the request is successful, with the expected calls", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 1)
				So(permissions.Required.Calls, ShouldEqual, 0)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
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

		Convey("Given a valid request is executed, but the firstUpdate call returns ErrDatasetNotFound", func() {
			mockedDataStore.UpdateVersionFunc = func(context.Context, *models.Version, *models.Version, string) (string, error) {
				So(isLocked, ShouldBeTrue)
				if len(mockedDataStore.UpdateVersionCalls()) == 1 {
					return "", errs.ErrDatasetNotFound
				}
				return "", nil
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			Convey("Then the request is successful, with the expected calls including the update retry", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 1)
				So(permissions.Required.Calls, ShouldEqual, 0)
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

	Convey("When state is set to associated", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionAssociatedPayload
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

		w := httptest.NewRecorder()

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

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

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
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

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
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

			api := GetAPIWithCantabularMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
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
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

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

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
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

			api := GetAPIWithCantabularMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
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

	datasetPermissions := getAuthorisationHandlerMock()
	permissions := getAuthorisationHandlerMock()
	api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
	api.Router.ServeHTTP(w, r)

	So(w.Code, ShouldEqual, http.StatusOK)
	So(datasetPermissions.Required.Calls, ShouldEqual, 1)
	So(permissions.Required.Calls, ShouldEqual, 0)
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

			datasetPermissions := getAuthorisationHandlerMock()
			permissions := getAuthorisationHandlerMock()

			api := GetAPIWithCMDMocks(mockedDataStore, mockDownloadGenerator, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			Convey("then an internal server error response is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})

			Convey("and the expected store calls are made with the expected parameters", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 1)
				So(permissions.Required.Calls, ShouldEqual, 0)

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

			datasetPermissions := getAuthorisationHandlerMock()
			permissions := getAuthorisationHandlerMock()
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and the updated version is as expected", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 1)
				So(permissions.Required.Calls, ShouldEqual, 0)
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

			datasetPermissions := getAuthorisationHandlerMock()
			permissions := getAuthorisationHandlerMock()
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 1)
				So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldEqual, "unauthenticated request\n")
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)

		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldEqual, "unable to update version as it has been published\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)

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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "missing collection_id for association between version and a collection\n")

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)

		So(permissions.Required.Calls, ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDeletePublishedVersionForbidden.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusMethodNotAllowed)
		So(w.Body.String(), ShouldContainSubstring, "method not allowed")

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
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
	})
}

func TestPutStateReturnsError(t *testing.T) {
	t.Parallel()

	Convey("When the request has an invalid version ID, return a bad request error", t, func() {
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/-123/state", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)

		api.putState(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())
	})

	Convey("When the request has an invalid body, return a bad request error", t, func() {
		b := `{"state":"invalid-body}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/1/state", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
	})

	Convey("When the request has an empty state, return a bad request error", t, func() {
		b := `{"state":""}`
		r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123-456/editions/678/versions/1/state", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)

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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)

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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)

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
			err := api.publishDistributionFiles(ctx, version, logData)

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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, "unable to update edition-id, invalid dataset type")
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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
