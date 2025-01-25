package api

import (
	"bytes"
	"context"
	"encoding/json"

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
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
	"github.com/smartystreets/goconvey/convey"
)

const (
	versionPayload           = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04"}`
	versionAssociatedPayload = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated","collection_id":"12345"}`
	versionPublishedPayload  = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"published","collection_id":"12345"}`
	testLockID               = "testLockID"
	testETag                 = "testETag"
)

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("get versions delegates offset and limit to db func and returns results list", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		results := []models.Version{}
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionsFunc: func(context.Context, string, string, string, int, int) ([]models.Version, int, error) {
				return results, 2, nil
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		list, totalCount, err := api.getVersions(w, r, 20, 0)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 1)
		convey.So(mockedDataStore.GetVersionsCalls()[0].Limit, convey.ShouldEqual, 20)
		convey.So(mockedDataStore.GetVersionsCalls()[0].Offset, convey.ShouldEqual, 0)
		convey.So(list, convey.ShouldResemble, results)
		convey.So(totalCount, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldEqual, nil)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()

	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrInternalServer
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		convey.So(err, convey.ShouldNotBeNil)

		assertInternalServerErr(w)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, _, err := api.getVersions(w, r, 20, 0)
		convey.So(err, convey.ShouldNotBeNil)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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
		convey.So(err, convey.ShouldNotBeNil)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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
		convey.So(err, convey.ShouldNotBeNil)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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
		convey.So(err, convey.ShouldNotBeNil)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When a published version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", http.NoBody)
		w := httptest.NewRecorder()

		version := models.Version{State: "gobbly-gook"}
		items := []models.Version{version}
		mockedDataStore := &storetest.StorerMock{
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrResourceState.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionsCalls()), convey.ShouldEqual, 1)
	})
}

func TestGetVersionReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a version", t, func() {
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

		convey.Convey("With an etag", func() {
			version.ETag = "version-etag"
			convey.Convey("When we call the GET version endpoint", func() {
				api.Router.ServeHTTP(w, r)

				convey.Convey("Then it returns a 200 OK", func() {
					convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				})
				convey.Convey("And the etag is returned in the response header", func() {
					convey.So(w.Header().Get("Etag"), convey.ShouldEqual, version.ETag)
				})

				convey.Convey("And the relevant calls have been made", func() {
					convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
					convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
					convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
					convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
					convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
				})
			})
		})
		convey.Convey("Without an etag", func() {
			version.ETag = ""
			convey.Convey("When we call the GET version endpoint", func() {
				api.Router.ServeHTTP(w, r)

				convey.Convey("Then it returns a 200 OK", func() {
					convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				})
				convey.Convey("And no etag is returned in the response header", func() {
					convey.So(w.Header().Get("Etag"), convey.ShouldBeEmpty)
				})

				convey.Convey("And the relevant calls have been made", func() {
					convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
					convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
					convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
					convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
					convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
				})
			})
		})
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When the dataset does not exist for return status not found", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
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
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When an invalid version is requested return invalid version error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/jjj", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("A request to get version zero returns an invalid version error response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/-1", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("A request to get a negative version returns an error response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/0", http.NoBody)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When an unpublished version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", http.NoBody)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrResourceState.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPutVersionReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	convey.Convey("When state is unchanged", t, func() {
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
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
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
				convey.So(isLocked, convey.ShouldBeTrue)
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

		convey.Convey("Given a valid request is executed", func() {
			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.Convey("Then the request is successful, with the expected calls", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateVersionCalls()[0].ETagSelector, convey.ShouldEqual, testETag)
				convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
				convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})

		convey.Convey("Given a valid request is executed, but the firstUpdate call returns ErrDatasetNotFound", func() {
			mockedDataStore.UpdateVersionFunc = func(context.Context, *models.Version, *models.Version, string) (string, error) {
				convey.So(isLocked, convey.ShouldBeTrue)
				if len(mockedDataStore.UpdateVersionCalls()) == 1 {
					return "", errs.ErrDatasetNotFound
				}
				return "", nil
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.Convey("Then the request is successful, with the expected calls including the update retry", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 3)
				convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 2)
				convey.So(mockedDataStore.UpdateVersionCalls()[0].ETagSelector, convey.ShouldEqual, testETag)
				convey.So(mockedDataStore.UpdateVersionCalls()[1].ETagSelector, convey.ShouldEqual, testETag)
				convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
				convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})
	})

	convey.Convey("When state is set to associated", t, func() {
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

		convey.Convey("put version with CMD type", func() {
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
						ID:   "789",
						Type: models.Filterable.String(),
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					convey.So(isLocked, convey.ShouldBeTrue)
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

			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
			convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
			convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
			convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 1)

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})

		convey.Convey("put version with Cantabular type and CMD mock", func() {
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{}, nil
				},
				CheckEditionExistsFunc: func(context.Context, string, string, string) error {
					return nil
				},
				GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
					return &models.Version{
						Type: "null",
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

			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
			convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
			convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
			convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
			convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})

		convey.Convey("put version with Cantabular type", func() {
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
						ID:   "789",
						Type: models.CantabularFlexibleTable.String(),
					}, nil
				},
				UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
					convey.So(isLocked, convey.ShouldBeTrue)
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

			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
			convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
			convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
			convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 1)

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})
	})

	convey.Convey("When state is set to edition-confirmed", t, func() {
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
					ID:    "789",
					State: models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				convey.So(isLocked, convey.ShouldBeTrue)
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 1)

		convey.Convey("Then the lock has been acquired and released exactly once", func() {
			validateLock(mockedDataStore, "789")
			convey.So(isLocked, convey.ShouldBeFalse)
		})

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When state is set to published", t, func() {
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

		convey.Convey("And the datatype is CMD", func() {
			isLocked := false
			mockedDataStore := &storetest.StorerMock{
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
					convey.So(isLocked, convey.ShouldBeTrue)
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
			}

			api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
			convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
			convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
			convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 1)
			convey.So(generatorMock.GenerateCalls()[0].Edition, convey.ShouldEqual, "2017")
			convey.So(generatorMock.GenerateCalls()[0].DatasetID, convey.ShouldEqual, "123")
			convey.So(generatorMock.GenerateCalls()[0].Version, convey.ShouldEqual, "1")
			convey.So(generatorMock.GenerateCalls()[0].InstanceID, convey.ShouldEqual, "789")

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})

		convey.Convey("And the datatype is Cantabular", func() {
			isLocked := false
			mockedDataStore := &storetest.StorerMock{
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
					convey.So(isLocked, convey.ShouldBeTrue)
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
			}

			api := GetAPIWithCantabularMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
			convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
			convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
			convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
			convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
			convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 1)
			convey.So(generatorMock.GenerateCalls()[0].Edition, convey.ShouldEqual, "2017")
			convey.So(generatorMock.GenerateCalls()[0].DatasetID, convey.ShouldEqual, "123")
			convey.So(generatorMock.GenerateCalls()[0].Version, convey.ShouldEqual, "1")
			convey.So(generatorMock.GenerateCalls()[0].InstanceID, convey.ShouldEqual, "789")

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})
	})

	convey.Convey("When version is already published and update includes downloads object only", t, func() {
		convey.Convey("And downloads object contains only a csv object", func() {
			b := `{"downloads": { "csv": { "public": "http://cmd-dev/test-site/cpih01", "size": "12", "href": "http://localhost:8080/cpih01"}}}`
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

			updateVersionDownloadTest(r)

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})

		convey.Convey("And downloads object contains only a xls object", func() {
			b := `{"downloads": { "xls": { "public": "http://cmd-dev/test-site/cpih01", "size": "12", "href": "http://localhost:8080/cpih01"}}}`
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))

			updateVersionDownloadTest(r)

			convey.Convey("then the request body has been drained", func() {
				_, err := r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
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
			convey.So(isLocked, convey.ShouldBeTrue)
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

	convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
	convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
	convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
	convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
	convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
	convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
	convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
	// Check updates to edition and dataset resources were not called
	convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
	convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
	convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
	convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

	convey.Convey("Then the lock has been acquired and released exactly once", func() {
		validateLock(mockedDataStore, "789")
		convey.So(isLocked, convey.ShouldBeFalse)
	})
}

func TestPutVersionGenerateDownloadsError(t *testing.T) {
	convey.Convey("given download generator returns an error", t, func() {
		mockedErr := errors.New("spectacular explosion")
		var v models.Version
		err := json.Unmarshal([]byte(versionAssociatedPayload), &v)
		convey.So(err, convey.ShouldBeNil)
		v.ID = "789"
		v.State = models.EditionConfirmedState

		isLocked := false
		mockedDataStore := &storetest.StorerMock{
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
				convey.So(isLocked, convey.ShouldBeTrue)
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

		convey.Convey("when put version is called with a valid request", func() {
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))

			w := httptest.NewRecorder()
			cfg, err := config.Get()
			convey.So(err, convey.ShouldBeNil)
			cfg.EnablePrivateEndpoints = true

			datasetPermissions := getAuthorisationHandlerMock()
			permissions := getAuthorisationHandlerMock()

			api := GetAPIWithCMDMocks(mockedDataStore, mockDownloadGenerator, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.Convey("then an internal server error response is returned", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
			})

			convey.Convey("and the expected store calls are made with the expected parameters", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

				genCalls := mockDownloadGenerator.GenerateCalls()

				convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetDatasetCalls()[0].ID, convey.ShouldEqual, "123")

				convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.CheckEditionExistsCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, convey.ShouldEqual, "2017")

				convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
				convey.So(mockedDataStore.GetVersionCalls()[0].DatasetID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetVersionCalls()[0].EditionID, convey.ShouldEqual, "2017")
				convey.So(mockedDataStore.GetVersionCalls()[0].Version, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)

				convey.So(len(genCalls), convey.ShouldEqual, 1)
				convey.So(genCalls[0].DatasetID, convey.ShouldEqual, "123")
				convey.So(genCalls[0].Edition, convey.ShouldEqual, "2017")
				convey.So(genCalls[0].Version, convey.ShouldEqual, "1")
			})

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})

			convey.Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				convey.So(err, convey.ShouldEqual, io.EOF)
			})
		})
	})
}

func TestPutEmptyVersion(t *testing.T) {
	getVersionAssociatedModel := func(datasetType models.DatasetType) models.Version {
		var v models.Version
		err := json.Unmarshal([]byte(versionAssociatedPayload), &v) //
		convey.So(err, convey.ShouldBeNil)                          //
		v.Type = datasetType.String()
		v.ID = "789"
		v.State = models.AssociatedState //
		return v
	}
	xlsDownload := &models.DownloadList{XLS: &models.DownloadObject{Size: "1", HRef: "/hello"}}

	// CMD
	convey.Convey("given an existing version with empty downloads", t, func() {
		v := getVersionAssociatedModel(models.Filterable)
		isLocked := false
		mockedDataStore := &storetest.StorerMock{
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
				convey.So(isLocked, convey.ShouldBeTrue)
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

		convey.Convey("when put version is called with an associated version with empty downloads", func() {
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			w := httptest.NewRecorder()

			datasetPermissions := getAuthorisationHandlerMock()
			permissions := getAuthorisationHandlerMock()
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.Convey("then a http status ok is returned", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			})

			convey.Convey("and the updated version is as expected", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, convey.ShouldBeNil)
			})

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})
		})
	})

	convey.Convey("given an existing version with a xls download already exists", t, func() {
		v := getVersionAssociatedModel(models.CantabularBlob)
		isLocked := false
		mockedDataStore := &storetest.StorerMock{
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
				convey.So(isLocked, convey.ShouldBeTrue)
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

		convey.Convey("when put version is called with an associated version with empty downloads", func() {
			r := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			w := httptest.NewRecorder()

			datasetPermissions := getAuthorisationHandlerMock()
			permissions := getAuthorisationHandlerMock()
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)

			convey.Convey("then a http status ok is returned", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
			})

			convey.Convey("and any existing version downloads are not overwritten", func() {
				convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, convey.ShouldResemble, xlsDownload)
			})

			convey.Convey("and the expected external calls are made with the correct parameters", func() {
				convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetDatasetCalls()[0].ID, convey.ShouldEqual, "123")

				convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.CheckEditionExistsCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, convey.ShouldEqual, "2017")
				convey.So(mockedDataStore.CheckEditionExistsCalls()[0].State, convey.ShouldEqual, "")

				convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
				convey.So(mockedDataStore.GetVersionCalls()[0].DatasetID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetVersionCalls()[0].EditionID, convey.ShouldEqual, "2017")
				convey.So(mockedDataStore.GetVersionCalls()[0].Version, convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetVersionCalls()[0].State, convey.ShouldEqual, "")

				convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockDownloadGenerator.GenerateCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("Then the lock has been acquired and released exactly once", func() {
				validateLock(mockedDataStore, "789")
				convey.So(isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestPutVersionReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the request contain malformed json a bad request status is returned", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request has negative version return invalid version error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request has zero version return invalid version error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When an request has invalid version return invalid version error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the dataset document cannot be found for version return status not found", t, func() {
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
				return &models.Version{}, errs.ErrVersionNotFound
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the edition document cannot be found for version return status not found", t, func() {
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
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the version document cannot be found return status not found", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request is not authorised to update version then response returns status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		b := versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		convey.So(err, convey.ShouldBeNil)
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
		convey.So(w.Body.String(), convey.ShouldEqual, "unauthenticated request\n")
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the version document has already been published return status forbidden", t, func() {
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
		convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
		convey.So(w.Body.String(), convey.ShouldEqual, "unable to update version as it has been published\n")

		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When the request body is invalid return status bad request", t, func() {
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
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:    "789",
					State: "associated",
				}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				convey.So(isLocked, convey.ShouldBeTrue)
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldEqual, "missing collection_id for association between version and a collection\n")

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("Then the lock has been acquired and released ", func() {
			validateLock(mockedDataStore, "789")
			convey.So(isLocked, convey.ShouldBeFalse)
		})

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})

	convey.Convey("When setting the instance node to published fails", t, func() {
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
				convey.So(isLocked, convey.ShouldBeTrue)
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 2)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)

		convey.Convey("Then the lock has been acquired and released ", func() {
			validateLock(mockedDataStore, "789")
			convey.So(isLocked, convey.ShouldBeFalse)
		})

		convey.Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			convey.So(err, convey.ShouldEqual, io.EOF)
		})
	})
}

func TestCreateNewVersionDoc(t *testing.T) {
	t.Parallel()
	convey.Convey("Given an empty current version and a version update that contains a collection_id", t, func() {
		currentVersion := models.Version{}
		versionUpdate := models.Version{
			CollectionID: "4321",
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version update contains the collection_id", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})
	})

	convey.Convey("Given a current version that contains a collection_id and a version update that contains a different collection_id", t, func() {
		currentVersion := models.Version{
			CollectionID: "1234",
		}
		versionUpdate := models.Version{
			CollectionID: "4321",
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version update contains the updated collection_id", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{
				CollectionID: "1234",
			})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{
				CollectionID: "4321",
			})
		})
	})

	convey.Convey("Given a current version that contains a collection_id and a version update that does not contain a collection_id", t, func() {
		currentVersion := models.Version{
			CollectionID: "1234",
		}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version update contains the updated collection_id", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				CollectionID: "1234",
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{
				CollectionID: "1234",
			})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{})
		})
	})

	convey.Convey("Given empty current version and update", t, func() {
		currentVersion := models.Version{}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version is empty", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{})
		})
	})

	convey.Convey("Given an empty current version and an update containing a spatial link", t, func() {
		currentVersion := models.Version{}
		versionUpdate := models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version contains the provided spatial link", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})
	})

	convey.Convey("Given a current version containing a spatial link and an update containing a different spatial link", t, func() {
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
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version contains the updated spatial link", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/oldgeographylist",
					},
				},
			})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/geographylist",
					},
				},
			})
		})
	})

	convey.Convey("Given a current version containing a spatial link and an empty update", t, func() {
		currentVersion := models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version contains the old spatial link", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/oldgeographylist",
					},
				},
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Spatial: &models.LinkObject{
						HRef: "http://ons.gov.uk/oldgeographylist",
					},
				},
			})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{})
		})
	})

	convey.Convey("Given a current version containing a dataset link and an empty update", t, func() {
		currentVersion := models.Version{
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://ons.gov.uk/datasets/123",
				},
			},
		}
		versionUpdate := models.Version{}
		combinedVersionUpdate, err := populateNewVersionDoc(&currentVersion, &versionUpdate)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("Then the combined version contains the old dataset link", func() {
			convey.So(*combinedVersionUpdate, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://ons.gov.uk/datasets/123",
					},
				},
			})
		})

		convey.Convey("And the existing variables did not mutate", func() {
			convey.So(currentVersion, convey.ShouldResemble, models.Version{
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://ons.gov.uk/datasets/123",
					},
				},
			})
			convey.So(versionUpdate, convey.ShouldResemble, models.Version{})
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

	convey.Convey("A successful detach request against a version of a published dataset returns 200 OK response.", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("A successful detach request against a version of a unpublished dataset returns 200 OK response.", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
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

	convey.Convey("When the api cannot connect to datastore return an internal server error.", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the provided edition cannot be found, return a 404 not found error.", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When detached is called against a version other than latest, return an internal server error", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When state is neither edition-confirmed or associated, return an internal server error", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the requested version cannot be found, return a not found error", t, func() {
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
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, generatorMock, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When updating the version fails, return an internal server error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When edition update fails whilst rolling back the edition, return an internal server error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When detached endpoint is called against an invalid version, return an invalid version error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When detached endpoint is called against a negative version, return an invalid version error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When detached endpoint is called against zeroq version, return an invalid version error", t, func() {
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 0)
		convey.So(len(generatorMock.GenerateCalls()), convey.ShouldEqual, 0)
	})
}

func assertInternalServerErr(w *httptest.ResponseRecorder) {
	convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
	convey.So(strings.TrimSpace(w.Body.String()), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	convey.So(mockedDataStore.AcquireInstanceLockCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, convey.ShouldEqual, expectedInstanceID)
	convey.So(mockedDataStore.UnlockInstanceCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.UnlockInstanceCalls()[0].LockID, convey.ShouldEqual, testLockID)
}
