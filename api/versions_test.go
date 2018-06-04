package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		p := common.Params{"dataset_id": "123-456", "edition": "678"}
		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()
	p := common.Params{"dataset_id": "123-456", "edition": "678"}

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "Incorrect resource state\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsAuditError(t *testing.T) {
	t.Parallel()
	p := common.Params{"dataset_id": "123-456", "edition": "678"}
	err := errors.New("error")

	Convey("when auditing get versions attempted action errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return err
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("when auditing check dataset exists error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return err
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionSuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		p := common.Params{"dataset_id": "123-456", "edition": "678"}
		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		p := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	p := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "Incorrect resource state\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionAuditErrors(t *testing.T) {
	p := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
	t.Parallel()
	Convey("When auditing get version action attempted errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionAttempted {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionSuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestPutVersionReturnsSuccessfully(t *testing.T) {
	ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
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
		mockedDataStore.GetVersion("123", "2017", "1", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
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
		mockedDataStore.GetVersion("123", "2017", "1", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})
		mockedDataStore.UpdateDatasetWithAssociation("123", models.AssociatedState, &models.Version{})

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		select {
		case <-downloadsGenerated:
			log.Info("download generated as expected", nil)
		case <-time.After(time.Second * 10):
			err := errors.New("failing test due to timeout")
			log.Error(err, nil)
			t.Fail()
		}

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 4)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
		verifyAuditRecordCalls(calls[2], associateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[3], associateVersionAction, actionSuccessful, ap)
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
					},
					Current: &models.Edition{},
				}, nil
			},
			UpsertEditionFunc: func(string, string, *models.EditionUpdate) error {
				return nil
			},
		}

		mockedDataStore.GetVersion("789", "2017", "1", "")
		mockedDataStore.GetEdition("123", "2017", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})
		mockedDataStore.GetDataset("123")
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{Next: &models.Dataset{}})

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 4)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
		verifyAuditRecordCalls(calls[2], publishVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[3], publishVersionAction, actionSuccessful, ap)
	})
}

func TestPutVersionGenerateDownloadsError(t *testing.T) {
	Convey("given download generator returns an error", t, func() {
		ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
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

			auditor := getMockAuditor()
			api := routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockDownloadGenerator, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

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

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 4)
				verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
				verifyAuditRecordCalls(calls[2], associateVersionAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[3], associateVersionAction, actionUnsuccessful, ap)
			})
		})
	})
}

func TestPutEmptyVersion(t *testing.T) {
	ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
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

			auditor := getMockAuditor()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and the updated version is as expected", func() {
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, ShouldBeNil)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
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

			auditor := getMockAuditor()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
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

				So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(mockDownloadGenerator.GenerateCalls()), ShouldEqual, 0)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
			})
		})
	})
}

func TestUpdateVersionAuditErrors(t *testing.T) {
	ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}

	versionDetails := versionDetails{
		datasetID: "123",
		edition:   "2017",
		version:   "1",
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
		},
		ReleaseDate: "2017",
		State:       models.EditionConfirmedState,
	}

	t.Parallel()

	Convey("given audit action attempted returns an error", t, func() {
		auditor := createAuditor(updateVersionAction, actionAttempted)

		Convey("when updateVersion is called with a valid request", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionPayload))
			So(err, ShouldBeNil)

			store := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(&storetest.StorerMock{}, nil, auditor, nil)

			currentDataset, currentVersion, updateVersion, err := api.updateVersion(r.Context(), r.Body, versionDetails)

			Convey("then an error is returned and updateVersion fails", func() {
				So(err, ShouldNotBeNil)
				So(currentDataset, ShouldBeNil)
				So(currentVersion, ShouldBeNil)
				So(updateVersion, ShouldBeNil)
				So(len(store.GetDatasetCalls()), ShouldEqual, 0)
				So(len(store.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(store.GetVersionCalls()), ShouldEqual, 0)
				So(len(store.UpdateVersionCalls()), ShouldEqual, 0)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 1)
				verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := createAuditor(updateVersionAction, actionSuccessful)

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

			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionPayload))
			So(err, ShouldBeNil)
			api := GetAPIWithMockedDatastore(store, nil, auditor, nil)

			actualCurrentDataset, actualCurrentVersion, actualUpdateVersion, err := api.updateVersion(r.Context(), r.Body, versionDetails)

			Convey("then the expected audit events are recorded and the expected error is returned", func() {
				So(err, ShouldBeNil)
				So(actualCurrentDataset, ShouldResemble, &models.DatasetUpdate{})
				So(actualCurrentVersion, ShouldResemble, currentVersion)
				So(actualUpdateVersion, ShouldResemble, &expectedUpdateVersion)
				So(len(store.GetDatasetCalls()), ShouldEqual, 1)
				So(len(store.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(store.GetVersionCalls()), ShouldEqual, 1)
				So(len(store.UpdateVersionCalls()), ShouldEqual, 1)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], updateVersionAction, actionSuccessful, ap)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := createAuditor(updateVersionAction, actionUnsuccessful)

		Convey("when update version is unsuccessful", func() {
			store := &storetest.StorerMock{
				GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionPayload))
			So(err, ShouldBeNil)
			api := GetAPIWithMockedDatastore(store, nil, auditor, nil)

			actualCurrentDataset, actualCurrentVersion, actualUpdateVersion, err := api.updateVersion(r.Context(), r.Body, versionDetails)

			Convey("then the expected audit events are recorded and the expected error is returned", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(actualCurrentDataset, ShouldBeNil)
				So(actualCurrentVersion, ShouldBeNil)
				So(actualUpdateVersion, ShouldBeNil)
				So(len(store.GetDatasetCalls()), ShouldEqual, 1)
				So(len(store.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(store.GetVersionCalls()), ShouldEqual, 0)
				So(len(store.UpdateVersionCalls()), ShouldEqual, 0)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], updateVersionAction, actionUnsuccessful, ap)
			})
		})
	})
}

func TestPutVersionReturnsError(t *testing.T) {
	ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "Failed to parse json body\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionUnsuccessful, ap)
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
				return nil, errInternal
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
		So(len(auditor.RecordCalls()), ShouldEqual, 0)
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dataset not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionUnsuccessful, ap)
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Edition not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionUnsuccessful, ap)
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Version not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)

		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionUnsuccessful, ap)
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldEqual, "unauthenticated request\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
		So(len(auditor.RecordCalls()), ShouldEqual, 0)
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

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldEqual, "unable to update version as it has been published\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(auditor.RecordCalls()), ShouldEqual, 0)
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
		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "Missing collection_id for association between version and a collection\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
		calls := auditor.RecordCalls()
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], updateVersionAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], updateVersionAction, actionUnsuccessful, ap)

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
