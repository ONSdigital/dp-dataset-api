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
	"github.com/ONSdigital/go-ns/common"
	. "github.com/smartystreets/goconvey/convey"
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
