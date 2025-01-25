package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/smartystreets/goconvey/convey"
)

func TestGetEditionsReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("get editions delegates offset and limit to db func and returns results list", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		publicResult := &models.Edition{ID: "20"}
		results := []*models.EditionUpdate{{Current: publicResult}}
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return results, 2, nil
			},
		}

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		list, totalCount, err := api.getEditions(w, r, 20, 0)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 1)
		convey.So(list, convey.ShouldResemble, []*models.Edition{publicResult})
		convey.So(totalCount, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldEqual, nil)
	})
}

func TestGetEditionsReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r.Header.Add("internal-token", "coffee")
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return nil, 0, errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return nil, 0, errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionsCalls()), convey.ShouldEqual, 1)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
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

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		r.Header.Add("internal-token", "coffee")
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
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When edition does not exist for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When edition is not published for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		convey.So(len(mockedDataStore.CheckDatasetExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
	})
}
