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
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	versionPayload           = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04"}`
	versionAssociatedPayload = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated","collection_id":"12345"}`
	versionPublishedPayload  = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"published","collection_id":"12345"}`
)

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get version returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}
		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()
	auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When a published version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()

		version := models.Version{State: "gobbly-gook"}
		items := []models.Version{version}
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{Items: items}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourceState.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetVersionsAuditError(t *testing.T) {
	t.Parallel()
	auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}
	err := errors.New("error")

	Convey("when auditing get versions attempted action errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return err
		}

		authHandler := getAuthorisationHandlerMock()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
		)
	})

	Convey("when auditing check dataset exists error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return err
			},
		}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == audit.Unsuccessful {
				return errors.New("error")
			}
			return nil
		}

		authHandler := getAuthorisationHandlerMock()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when auditing check edition exists error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return err
			},
		}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == audit.Unsuccessful {
				return errors.New("error")
			}
			return nil
		}

		authHandler := getAuthorisationHandlerMock()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when auditing get versions error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID string, editionID string, state string) (*models.VersionResults, error) {
				return nil, err
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.NewErroring(getVersionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when auditing invalid state returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID string, editionID string, state string) (*models.VersionResults, error) {
				return &models.VersionResults{
					Items: []models.Version{{State: "not valid"}},
				}, nil
			},
		}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == audit.Unsuccessful {
				return errors.New("error")
			}
			return nil
		}

		authHandler := getAuthorisationHandlerMock()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when auditing get versions successful event errors then an 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == audit.Successful {
				return errors.New("error")
			}
			return nil
		}

		authHandler := getAuthorisationHandlerMock()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)

		auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}
		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionsAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get version returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: models.EditionConfirmedState,
					Links: &models.VersionLinks{
						Self: &models.LinkObject{},
						Version: &models.LinkObject{
							HRef: "href",
						},
					},
				}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		auditParams := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the dataset does not exist for return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When an unpublished version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
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

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourceState.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetVersionAuditErrors(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
	t.Parallel()
	Convey("When auditing get version action attempted errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrInternalServer
			},
		}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == audit.Attempted {
				return errors.New("error")
			}
			return nil
		}

		authHandler := getAuthorisationHandlerMock()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
		)
	})

	Convey("When the dataset does not exist and audit errors then return a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.NewErroring(getVersionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the edition does not exist for a dataset and auditing errors then a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.NewErroring(getVersionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When version does not exist for an edition of a dataset and auditing errors then a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.NewErroring(getVersionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When version does not exist for an edition of a dataset and auditing errors then a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: "indifferent",
					Links: &models.VersionLinks{
						Self:    &models.LinkObject{HRef: "self"},
						Version: &models.LinkObject{HRef: "version"},
					},
				}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.NewErroring(getVersionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when auditing a successful request to get a version errors then return a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: models.EditionConfirmedState,
					Links: &models.VersionLinks{
						Self: &models.LinkObject{},
						Version: &models.LinkObject{
							HRef: "href",
						},
					},
				}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.NewErroring(getVersionAction, audit.Successful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getVersionAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestPutVersionReturnsSuccessfully(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}

	t.Parallel()
	Convey("When state is unchanged", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
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
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When state is set to associated", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionAssociatedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.AssociatedState,
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			UpdateDatasetWithAssociationFunc: func(string, string, *models.Version) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When state is set to edition-confirmed", t, func() {
		downloadsGenerated := make(chan bool, 1)

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				downloadsGenerated <- true
				return nil
			},
		}

		var b string
		b = versionAssociatedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			UpdateDatasetWithAssociationFunc: func(string, string, *models.Version) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		select {
		case <-downloadsGenerated:
			log.Info("download generated as expected", nil)
		case <-time.After(time.Second * 10):
			err := errors.New("failing test due to timeout")
			log.Error(err, nil)
			t.Fail()
		}

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
			auditortest.Expected{Action: associateVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: associateVersionAction, Result: audit.Successful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When state is set to published", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPublishedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
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
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Next:    &models.Dataset{Links: &models.DatasetLinks{}},
					Current: &models.Dataset{Links: &models.DatasetLinks{}},
				}, nil
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
			GetEditionFunc: func(string, string, string) (*models.EditionUpdate, error) {
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
			UpsertEditionFunc: func(string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(ctx context.Context, instanceID string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
			auditortest.Expected{Action: publishVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: publishVersionAction, Result: audit.Successful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When version is already published and update includes downloads object only", t, func() {
		Convey("And downloads object contains only a csv object", func() {
			var b string
			b = `{"downloads": { "csv": { "public": "http://cmd-dev/test-site/cpih01", "size": "12", "href": "http://localhost:8080/cpih01"}}}`
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
			So(err, ShouldBeNil)

			updateVersionDownloadTest(r, auditParamsWithCallerIdentity, auditParams)

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("And downloads object contains only a xls object", func() {
			var b string
			b = `{"downloads": { "xls": { "public": "http://cmd-dev/test-site/cpih01", "size": "12", "href": "http://localhost:8080/cpih01"}}}`
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
			So(err, ShouldBeNil)

			updateVersionDownloadTest(r, auditParamsWithCallerIdentity, auditParams)

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})
}

func updateVersionDownloadTest(r *http.Request, firstAuditParams, secondAuditParams common.Params) {
	w := httptest.NewRecorder()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(string, string, string, string) error {
			return nil
		},
	}

	mockedDataStore := &storetest.StorerMock{
		GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
			return &models.DatasetUpdate{
				ID:      "123",
				Next:    &models.Dataset{Links: &models.DatasetLinks{}},
				Current: &models.Dataset{Links: &models.DatasetLinks{}},
			}, nil
		},
		CheckEditionExistsFunc: func(string, string, string) error {
			return nil
		},
		GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
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
		UpdateVersionFunc: func(string, *models.Version) error {
			return nil
		},
		GetEditionFunc: func(string, string, string) (*models.EditionUpdate, error) {
			return &models.EditionUpdate{
				ID: "123",
				Next: &models.Edition{
					State: models.PublishedState,
				},
				Current: &models.Edition{},
			}, nil
		},
	}

	authHandler := getAuthorisationHandlerMock()
	auditor := auditortest.New()
	api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
	api.Router.ServeHTTP(w, r)

	So(w.Code, ShouldEqual, http.StatusOK)
	So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
	So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
	So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
	So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
	// Check updates to edition and dataset resources were not called
	So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
	So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
	So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

	auditor.AssertRecordCalls(
		auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: firstAuditParams},
		auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: secondAuditParams},
	)
}

func TestPutVersionGenerateDownloadsError(t *testing.T) {
	Convey("given download generator returns an error", t, func() {
		auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
		auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}

		mockedErr := errors.New("spectacular explosion")
		var v models.Version
		json.Unmarshal([]byte(versionAssociatedPayload), &v)
		v.State = models.EditionConfirmedState

		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return &v, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
			UpdateDatasetWithAssociationFunc: func(ID string, state string, version *models.Version) error {
				return nil
			},
		}

		mockDownloadGenerator := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return mockedErr
			},
		}

		Convey("when put version is called with a valid request", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			cfg, err := config.Get()
			So(err, ShouldBeNil)
			cfg.EnablePrivateEnpoints = true

			auditor := auditortest.New()
			api := Routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockDownloadGenerator, auditor)
			api.Router.ServeHTTP(w, r)

			Convey("then an internal server error response is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})

			Convey("and the expected store calls are made with the expected parameters", func() {
				genCalls := mockDownloadGenerator.GenerateCalls()

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(mockedDataStore.GetDatasetCalls()[0].ID, ShouldEqual, "123")

				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(mockedDataStore.CheckEditionExistsCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, ShouldEqual, "2017")

				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetVersionCalls()[0].DatasetID, ShouldEqual, "123")
				So(mockedDataStore.GetVersionCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.GetVersionCalls()[0].Version, ShouldEqual, "1")
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)

				So(len(genCalls), ShouldEqual, 1)
				So(genCalls[0].DatasetID, ShouldEqual, "123")
				So(genCalls[0].Edition, ShouldEqual, "2017")
				So(genCalls[0].Version, ShouldEqual, "1")

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
					auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
					auditortest.Expected{Action: associateVersionAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: associateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})
}

func TestPutEmptyVersion(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}

	var v models.Version
	json.Unmarshal([]byte(versionAssociatedPayload), &v)
	v.State = models.AssociatedState
	xlsDownload := &models.DownloadList{XLS: &models.DownloadObject{Size: "1", HRef: "/hello"}}

	Convey("given an existing version with empty downloads", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return &v, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
		}

		Convey("when put version is called with an associated version with empty downloads", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()

			authHandler := getAuthorisationHandlerMock()
			auditor := auditortest.New()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and the updated version is as expected", func() {
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, ShouldBeNil)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
					auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})

	Convey("given an existing version with a xls download already exists", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				v.Downloads = xlsDownload
				return &v, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
		}

		mockDownloadGenerator := &mocks.DownloadsGeneratorMock{}

		Convey("when put version is called with an associated version with empty downloads", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			authHandler := getAuthorisationHandlerMock()
			auditor := auditortest.New()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
			})

			Convey("and any existing version downloads are not overwritten", func() {
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, ShouldResemble, xlsDownload)
			})

			Convey("and the expected external calls are made with the correct parameters", func() {
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(mockedDataStore.GetDatasetCalls()[0].ID, ShouldEqual, "123")

				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(mockedDataStore.CheckEditionExistsCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.CheckEditionExistsCalls()[0].State, ShouldEqual, "")

				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetVersionCalls()[0].DatasetID, ShouldEqual, "123")
				So(mockedDataStore.GetVersionCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.GetVersionCalls()[0].Version, ShouldEqual, "1")
				So(mockedDataStore.GetVersionCalls()[0].State, ShouldEqual, "")

				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(mockDownloadGenerator.GenerateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
					auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})
}

func TestUpdateVersionAuditErrors(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}

	t.Parallel()
	Convey("given audit action attempted returns an error", t, func() {
		auditor := auditortest.NewErroring(updateVersionAction, audit.Attempted)

		Convey("when updateVersion is called with a valid request", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()

			store := &storetest.StorerMock{}
			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, nil, auditor, authHandler)

			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusInternalServerError)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

			So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

			Convey("then an error is returned and updateVersion fails", func() {
				// Check no calls have been made to the datastore
				So(len(store.GetDatasetCalls()), ShouldEqual, 0)
				So(len(store.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(store.GetVersionCalls()), ShouldEqual, 0)
				So(len(store.UpdateVersionCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	currentVersion := &models.Version{
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
		ReleaseDate: "2017",
		State:       models.EditionConfirmedState,
	}

	Convey("given audit action successful returns an error", t, func() {
		auditor := auditortest.NewErroring(updateVersionAction, audit.Successful)

		Convey("when updateVersion is called with a valid request", func() {

			store := &storetest.StorerMock{
				GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{}, nil
				},
				CheckEditionExistsFunc: func(string, string, string) error {
					return nil
				},
				GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
					return currentVersion, nil
				},
				UpdateVersionFunc: func(string, *models.Version) error {
					return nil
				},
			}

			var expectedUpdateVersion models.Version
			err := json.Unmarshal([]byte(versionPayload), &expectedUpdateVersion)
			So(err, ShouldBeNil)
			expectedUpdateVersion.Downloads = currentVersion.Downloads
			expectedUpdateVersion.Links = currentVersion.Links
			expectedUpdateVersion.ID = currentVersion.ID
			expectedUpdateVersion.State = models.EditionConfirmedState

			w := httptest.NewRecorder()

			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionPayload))
			So(err, ShouldBeNil)

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, nil, auditor, authHandler)
			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)

			Convey("then the expected audit events are recorded and the expected error is returned", func() {
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(store.GetDatasetCalls()), ShouldEqual, 1)
				So(len(store.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(store.GetVersionCalls()), ShouldEqual, 2)
				So(len(store.UpdateVersionCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
					auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := auditortest.NewErroring(updateVersionAction, audit.Unsuccessful)

		Convey("when update version is unsuccessful", func() {
			store := &storetest.StorerMock{
				GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
					return nil, errs.ErrVersionNotFound
				},
				GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, nil, auditor, authHandler)
			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusNotFound)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
			So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)

			Convey("then the expected audit events are recorded and the expected error is returned", func() {
				So(len(store.GetVersionCalls()), ShouldEqual, 1)
				So(len(store.GetDatasetCalls()), ShouldEqual, 1)
				So(len(store.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(store.UpdateVersionCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
					auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})
	})
}

func TestPublishVersionAuditErrors(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	versionDetails := VersionDetails{
		datasetID: "123",
		edition:   "2017",
		version:   "1",
	}

	Convey("given audit action attempted returns an error", t, func() {
		auditor := auditortest.NewErroring(publishVersionAction, audit.Attempted)

		Convey("when publish version is called", func() {
			store := &storetest.StorerMock{}
			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, nil, auditor, authHandler)

			err := api.publishVersion(context.Background(), nil, nil, nil, versionDetails)
			So(err, ShouldNotBeNil)

			Convey("then the expected audit events are recorded and an error is returned", func() {
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
				auditor.AssertRecordCalls(
					auditortest.Expected{Action: publishVersionAction, Result: audit.Attempted, Params: auditParams},
				)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := auditortest.NewErroring(publishVersionAction, audit.Unsuccessful)

		Convey("when publish version returns an error", func() {
			store := &storetest.StorerMock{
				GetEditionFunc: func(ID, editionID, state string) (*models.EditionUpdate, error) {
					return nil, errs.ErrEditionNotFound
				},
			}

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, nil, auditor, authHandler)
			err := api.publishVersion(context.Background(), nil, nil, nil, versionDetails)
			So(err, ShouldNotBeNil)

			Convey("then the expected audit events are recorded and the expected error is returned", func() {
				So(len(store.GetEditionCalls()), ShouldEqual, 1)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: publishVersionAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: publishVersionAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := auditortest.NewErroring(publishVersionAction, audit.Successful)

		Convey("when publish version returns an error", func() {
			store := &storetest.StorerMock{
				GetEditionFunc: func(string, string, string) (*models.EditionUpdate, error) {
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
				UpsertEditionFunc: func(datasetID string, edition string, editionDoc *models.EditionUpdate) error {
					return nil
				},
				UpsertDatasetFunc: func(ID string, datasetDoc *models.DatasetUpdate) error {
					return nil
				},
				SetInstanceIsPublishedFunc: func(ctx context.Context, instanceID string) error {
					return nil
				},
			}

			currentDataset := &models.DatasetUpdate{
				ID:      "123",
				Next:    &models.Dataset{Links: &models.DatasetLinks{}},
				Current: &models.Dataset{Links: &models.DatasetLinks{}},
			}

			currentVersion := &models.Version{
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
					Version: &models.LinkObject{
						HRef: "",
						ID:   "1",
					},
				},
				ReleaseDate: "2017-12-12",
				State:       models.EditionConfirmedState,
			}

			var updateVersion models.Version
			err := json.Unmarshal([]byte(versionPublishedPayload), &updateVersion)
			So(err, ShouldBeNil)
			updateVersion.Links = currentVersion.Links

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, nil, auditor, authHandler)

			err = api.publishVersion(context.Background(), currentDataset, currentVersion, &updateVersion, versionDetails)
			So(err, ShouldBeNil)

			Convey("then the expected audit events are recorded and the expected error is returned", func() {
				So(len(store.GetEditionCalls()), ShouldEqual, 1)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: publishVersionAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: publishVersionAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})
}

func TestAssociateVersionAuditErrors(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123", "edition": "2018", "version": "1"}
	currentVersion := &models.Version{
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
			Version: &models.LinkObject{
				HRef: "",
			},
		},
		ReleaseDate: "2017-12-12",
		State:       models.EditionConfirmedState,
	}

	var versionDoc models.Version
	json.Unmarshal([]byte(versionAssociatedPayload), &versionDoc)

	versionDetails := VersionDetails{
		datasetID: "123",
		edition:   "2018",
		version:   "1",
	}

	expectedErr := errors.New("err")

	Convey("given audit action attempted returns an error", t, func() {
		auditor := auditortest.NewErroring(associateVersionAction, audit.Attempted)

		Convey("when associate version is called", func() {

			store := &storetest.StorerMock{}
			gen := &mocks.DownloadsGeneratorMock{}
			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, gen, auditor, authHandler)

			err := api.associateVersion(context.Background(), currentVersion, &versionDoc, versionDetails)
			So(err, ShouldEqual, auditortest.ErrAudit)

			Convey("then the expected audit event is captured and the expected error is returned", func() {
				So(len(store.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(gen.GenerateCalls()), ShouldEqual, 0)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: associateVersionAction, Result: audit.Attempted, Params: auditParams},
				)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := auditortest.NewErroring(associateVersionAction, audit.Unsuccessful)

		Convey("when datastore.UpdateDatasetWithAssociation returns an error", func() {
			store := &storetest.StorerMock{
				UpdateDatasetWithAssociationFunc: func(ID string, state string, version *models.Version) error {
					return expectedErr
				},
			}
			gen := &mocks.DownloadsGeneratorMock{}
			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, gen, auditor, authHandler)

			err := api.associateVersion(context.Background(), currentVersion, &versionDoc, versionDetails)
			So(err, ShouldEqual, expectedErr)

			Convey("then the expected audit event is captured and the expected error is returned", func() {
				So(len(store.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
				So(len(gen.GenerateCalls()), ShouldEqual, 0)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: associateVersionAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: associateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})

		Convey("when generating downloads returns an error", func() {
			store := &storetest.StorerMock{
				UpdateDatasetWithAssociationFunc: func(ID string, state string, version *models.Version) error {
					return nil
				},
			}
			gen := &mocks.DownloadsGeneratorMock{
				GenerateFunc: func(datasetID string, instanceID string, edition string, version string) error {
					return expectedErr
				},
			}

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, gen, auditor, authHandler)

			err := api.associateVersion(context.Background(), currentVersion, &versionDoc, versionDetails)

			Convey("then the expected audit event is captured and the expected error is returned", func() {
				So(expectedErr.Error(), ShouldEqual, errors.Cause(err).Error())
				So(len(store.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
				So(len(gen.GenerateCalls()), ShouldEqual, 1)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: associateVersionAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: associateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := auditortest.NewErroring(associateVersionAction, audit.Successful)

		Convey("when associateVersion is called", func() {
			store := &storetest.StorerMock{
				UpdateDatasetWithAssociationFunc: func(ID string, state string, version *models.Version) error {
					return nil
				},
			}
			gen := &mocks.DownloadsGeneratorMock{
				GenerateFunc: func(datasetID string, instanceID string, edition string, version string) error {
					return nil
				},
			}

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(store, gen, auditor, authHandler)
			err := api.associateVersion(context.Background(), currentVersion, &versionDoc, versionDetails)
			So(err, ShouldBeNil)

			Convey("then the expected audit event is captured and the expected error is returned", func() {
				So(len(store.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
				So(len(gen.GenerateCalls()), ShouldEqual, 1)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{Action: associateVersionAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: associateVersionAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})
}

func TestPutVersionReturnsError(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = "{"
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the dataset document cannot be found for version return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(datasetID string, edition string, versionID string, version string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the edition document cannot be found for version return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the version document cannot be found return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request is not authorised to update version then response returns status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: "associated",
				}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldEqual, "unauthenticated request\n")
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the version document has already been published return status forbidden", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.PublishedState,
				}, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldEqual, "unable to update version as it has been published\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request body is invalid return status bad request", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated"}`
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{State: "associated"}, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "missing collection_id for association between version and a collection\n")

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When setting the instance node to published fails", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPublishedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
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
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Next:    &models.Dataset{Links: &models.DatasetLinks{}},
					Current: &models.Dataset{Links: &models.DatasetLinks{}},
				}, nil
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
			GetEditionFunc: func(string, string, string) (*models.EditionUpdate, error) {
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
			UpsertEditionFunc: func(string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(ctx context.Context, instanceID string) error {
				return errors.New("failed to set is_published on the instance node")
			},
		}

		mockedDataStore.GetVersion("789", "2017", "1", "")
		mockedDataStore.GetEdition("123", "2017", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})
		mockedDataStore.GetDataset("123")
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{Next: &models.Dataset{}})

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: updateVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: updateVersionAction, Result: audit.Successful, Params: auditParams},
			auditortest.Expected{Action: publishVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: publishVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestCreateNewVersionDoc(t *testing.T) {
	t.Parallel()
	Convey("Check the version has the new collection id when request contains a collection_id", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{
			CollectionID: "4321",
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "4321")
	})

	Convey("Check the version collection id does not get replaced by the current collection id when request contains a collection_id", t, func() {
		currentVersion := &models.Version{
			CollectionID: "1234",
		}
		version := &models.Version{
			CollectionID: "4321",
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "4321")
	})

	Convey("Check the version has the old collection id when request is missing a collection_id", t, func() {
		currentVersion := &models.Version{
			CollectionID: "1234",
		}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "1234")
	})

	Convey("check the version collection id is not set when both request body and current version document are missing a collection id", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "")
	})

	Convey("Check the version has the new spatial link when request contains a links.spatial.href", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldNotBeNil)
		So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/geographylist")
	})

	Convey("Check the version links.spatial.href does not get replaced by the current version value", t, func() {
		currentVersion := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		version := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldNotBeNil)
		So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/geographylist")
	})

	Convey("Check the links.spatial.href has the old value when request does not contain a links.spatial.href", t, func() {
		currentVersion := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldNotBeNil)
		So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/oldgeographylist")
	})

	Convey("check the version links.spatial.href is not set when both request body and current version document do not contain a links.spatial.href", t, func() {
		currentVersion := &models.Version{
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://ons.gov.uk/datasets/123",
				},
			},
		}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldBeNil)
	})
}

func TestDetachVersionReturnOK(t *testing.T) {

	// TODO conditional test for feature flagged functionality. Will need tidying up eventually.
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if !featureOn {
		return
	}

	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}
	t.Parallel()

	Convey("A successful detach request against a version of a published dataset returns 200 OK response.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
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
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{}}, nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
			UpsertEditionFunc: func(datasetID string, edition string, editionDoc *models.EditionUpdate) error {
				return nil
			},
			UpsertDatasetFunc: func(ID string, datasetDoc *models.DatasetUpdate) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Successful, Params: auditParams},
		)
	})

	Convey("A successful detach request against a version of a unpublished dataset returns 200 OK response.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
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
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
			UpsertEditionFunc: func(datasetID string, edition string, editionDoc *models.EditionUpdate) error {
				return nil
			},
			UpsertDatasetFunc: func(ID string, datasetDoc *models.DatasetUpdate) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestDetachVersionReturnsError(t *testing.T) {

	// TODO conditional test for feature flagged functionality. Will need tidying up eventually.
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if !featureOn {
		return
	}

	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123", "edition": "2017", "version": "1"}
	t.Parallel()

	Convey("When the api cannot connect to datastore return an internal server error.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the provided edition cannot be found, return a 404 not found error.", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When detached is called against a version other than latest, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "2"}}}}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When state is neither edition-confirmed or associated, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.PublishedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the requested version cannot be found, return a not found error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When updating the version fails, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},

			GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},

			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When edition update fails whilst rolling back the edition, return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123/editions/2017/versions/1", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(datasetID, editionID, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{LatestVersion: &models.LinkObject{ID: "1"}}}}, nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{}, nil
			},

			GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{}}, nil
			},

			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
			UpsertEditionFunc: func(datasetID string, edition string, editionDoc *models.EditionUpdate) error {
				return errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, authHandler)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParamsWithCallerIdentity},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: detachVersionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

}

func assertInternalServerErr(w *httptest.ResponseRecorder) {
	So(w.Code, ShouldEqual, http.StatusInternalServerError)
	So(strings.TrimSpace(w.Body.String()), ShouldEqual, errs.ErrInternalServer.Error())
}
