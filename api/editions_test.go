package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	"github.com/ONSdigital/go-ns/common"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	editionPayload = `{"edition":"2017","state":"created"}`
)

func TestGetEditionsReturnsOK(t *testing.T) {

	t.Parallel()
	Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)

		auditParams := common.Params{"dataset_id": "123-456"}
		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetEditionsAuditingError(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456"}

	t.Parallel()
	Convey("given auditing get editions attempted action returns an error then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
		}

		auditor := auditortest.New()
		auditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("get editions action attempted audit event error")
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{
				Action: getEditionsAction,
				Result: audit.Attempted,
				Params: auditParams,
			},
		)
	})

	Convey("given auditing get editions action successful returns an error then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
		}

		auditor := auditortest.NewErroring(getEditionsAction, audit.Successful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Successful, Params: auditParams},
		)
	})

	Convey("When the dataset does not exist and auditing the action result fails then return status 500", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditor := auditortest.NewErroring(getEditionsAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When no published editions exist against a published dataset and auditing unsuccessful errors return status 500", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditor := auditortest.NewErroring(getEditionsAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetEditionsReturnsError(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456"}
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrInternalServer
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionsAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{}, nil
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)

		auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}
		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}

	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrInternalServer
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When edition does not exist for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When edition is not published for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditor := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetEditionAuditErrors(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456", "edition": "678"}

	t.Parallel()
	Convey("when auditing get edition attempted action errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		auditor := auditortest.NewErroring(getEditionAction, audit.Attempted)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{
				Action: getEditionAction,
				Result: audit.Attempted,
				Params: auditParams,
			},
		)
	})

	Convey("when check dataset exists errors and auditing action unsuccessful errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return errors.New("check dataset error")
			},
		}

		auditor := auditortest.NewErroring(getEditionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when check edition exists errors and auditing action unsuccessful errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			GetEditionFunc: func(ID, editionID, state string) (*models.EditionUpdate, error) {
				return nil, errors.New("get edition error")
			},
		}

		auditor := auditortest.NewErroring(getEditionAction, audit.Unsuccessful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("when get edition audit even successful errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{}, nil
			},
		}

		auditor := auditortest.NewErroring(getEditionAction, audit.Successful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			auditortest.Expected{Action: getEditionAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getEditionAction, Result: audit.Successful, Params: auditParams},
		)
	})
}
