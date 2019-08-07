package instance_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	"github.com/ONSdigital/go-ns/common"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_InsertedObservationsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with inserted observations", t, func() {
		Convey("When the request is authorised", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateObservationInsertedFunc: func(id string, ob int64) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "200", "instance_id": "123"}},
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Successful, common.Params{"instance_id": "123", "inserted_observations": "200"}},
				)
			})
		})
	})
}

func Test_InsertedObservationsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with inserted observations", t, func() {
		Convey("When the service is unable to connect to the datastore", func() {
			Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "200", "instance_id": "123"}},
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Unsuccessful, common.Params{"instance_id": "123"}},
				)
			})
		})

		Convey("When the instance no longer exists after validating instance state", func() {
			Convey("Then return status not found (404)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateObservationInsertedFunc: func(id string, ob int64) error {
						return errs.ErrInstanceNotFound
					},
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "200", "instance_id": "123"}},
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Unsuccessful, common.Params{"instance_id": "123", "inserted_observations": "200"}},
				)
			})
		})

		Convey("When the request parameter 'inserted_observations' is not an integer value", func() {
			Convey("Then return status bad request (400)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/aa12a", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.SubmittedState}, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInsertedObservationsInvalidSyntax.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "aa12a", "instance_id": "123"}},
					auditortest.Expected{instance.UpdateInsertedObservationsAction, audit.Unsuccessful, common.Params{"instance_id": "123", "inserted_observations": "aa12a"}},
				)
			})
		})
	})
}

func Test_InsertedObservations_AuditFailure(t *testing.T) {
	t.Parallel()
	Convey("Given a request to update instance resource with inserted observations is made", t, func() {
		Convey(`When the subsequent audit action 'attempted' fails`, func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}
			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()
			auditor := auditortest.NewErroring(instance.UpdateInsertedObservationsAction, audit.Attempted)

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 0)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

				auditParams := common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "200", "instance_id": "123"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateInsertedObservationsAction, audit.Attempted, auditParams),
				)
			})
		})

		Convey(`When the request parameter 'inserted_observations' is not an integer
			 and the subsequent audit action 'unsuccessful' fails`, func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/1.5", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
			}

			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()
			auditor := auditortest.NewErroring(instance.UpdateInsertedObservationsAction, audit.Unsuccessful)

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateInsertedObservationsAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "1.5", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateInsertedObservationsAction, audit.Unsuccessful, common.Params{"instance_id": "123", "inserted_observations": "1.5"}),
				)
			})
		})

		Convey(`When the request successfully updates instance resource but
			the subsequent audit action 'successful' fails`, func() {

			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
				UpdateObservationInsertedFunc: func(id string, observations int64) error {
					return nil
				},
			}

			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()
			auditor := auditortest.NewErroring(instance.UpdateInsertedObservationsAction, audit.Unsuccessful)

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 200 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateInsertedObservationsAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "inserted_observations": "200", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateInsertedObservationsAction, audit.Successful, common.Params{"instance_id": "123", "inserted_observations": "200"}),
				)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateImportObservationsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import observations", t, func() {
		Convey("When the request is authorised", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"completed"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Successful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})
}

func Test_UpdateImportTaskRetrunsError(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task", t, func() {
		Convey("When the service is unable to connect to the datastore", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
			})
		})

		Convey("When the instance resource does not exist", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
			})
		})

		Convey("When the instance resource is already published", func() {
			Convey("Then return status forbidden (403)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.PublishedState}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateInstanceAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateInstanceAction, audit.Unsuccessful, common.Params{"instance_id": "123", "instance_state": models.PublishedState}),
				)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateImportObservationsReturnsError(t *testing.T) {
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
	auditParams := common.Params{"instance_id": "123"}

	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import observations", t, func() {
		Convey("When the request body contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unexpected end of JSON input")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body is missing mandatory field, 'state'", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"import_observations":{}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid import observation task, must include state")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body contains an invalid 'state' value", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"notvalid"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
						return nil
					},
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value for import observations: notvalid")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the service loses connection to datastore whilst updating observations", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"completed"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
						return errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})
	})
}

func Test_UpdateImportTask_BuildHierarchyTaskReturnsError(t *testing.T) {
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
	auditParams := common.Params{"instance_id": "123"}

	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task 'build hierarchies'", t, func() {
		Convey("When the request body contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unexpected end of JSON input")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body contains empty json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - request body does not contain any import tasks")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body contains empty 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing hierarchy task")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body is missing 'dimension_name' from 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body is missing 'state' from 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the import task has an invalid 'state' value inside the 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"notvalid", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value: notvalid")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the import task has the incorrect 'dimension_name' value in the 'build_hierarchies' object", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return errors.New("not found")
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, "geography hierarchy import task does not exist")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When service loses connection to datastore while updating resource", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return errors.New("internal error")
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})
	})
}

func Test_UpdateImportTask_BuildHierarchyTaskReturnsOk(t *testing.T) {

	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task 'build hierarchies'", t, func() {
		Convey("When the request body is valid", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState}, nil
					},
					UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Successful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateBuildSearchIndexTask_Failure(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task 'build search indexes'", t, func() {
		Convey("When the request body contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unexpected end of JSON input")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body contains empty json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - request body does not contain any import tasks")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body contains empty 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing search index task")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body is missing 'dimension_name' from 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the request body is missing 'state' from 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the import task has an invalid 'state' value inside the 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"notvalid", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value: notvalid")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the import task has the incorrect 'dimension_name' value in the 'build_search_indexes' object", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return errors.New("not found")
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()
				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, "geography search index import task does not exist")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When service loses connection to datastore while updating resource", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return errors.New("internal error")
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateBuildSearchIndexReturnsOk(t *testing.T) {

	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task 'build_search_indexes'", t, func() {
		Convey("When the request body is valid", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
						return nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				auditor := auditortest.New()

				datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Successful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})
}

func Test_UpdateImportTask_AuditAttemptFailure(t *testing.T) {
	t.Parallel()
	Convey("Given audit action attempted returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.UpdateImportTasksAction, audit.Attempted)

		Convey("When update import task is called", func() {
			body := strings.NewReader(`{"build_search_indexes":[{"state":"completed"}]}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}
			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 0)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditParams := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParams),
				)
			})
		})
	})
}

func Test_UpdateImportTask_AuditUnsuccessfulError(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

	t.Parallel()
	Convey("Given audit action unsuccessful returns an error", t, func() {
		Convey("When the request body fails to marshal into the updateImportTask model", func() {
			auditor := auditortest.NewErroring(instance.UpdateImportTasksAction, audit.Unsuccessful)
			body := strings.NewReader(`THIS IS NOT JSON`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
			}

			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditParams := common.Params{"instance_id": "123"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When UpdateImportObservationsTaskState returns an error", func() {
			auditor := auditortest.NewErroring(instance.UpdateImportTasksAction, audit.Unsuccessful)
			body := strings.NewReader(`{"import_observations":{"state":"completed"}}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
				UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
					return errors.New("error")
				},
			}

			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When UpdateBuildHierarchyTaskState returns an error", func() {
			auditor := auditortest.NewErroring(instance.UpdateImportTasksAction, audit.Unsuccessful)
			body := strings.NewReader(`{"build_hierarchies":[{"dimension_name": "geography", "state":"completed"}]}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
				UpdateBuildHierarchyTaskStateFunc: func(id string, dimension string, state string) error {
					return errors.New("error")
				},
			}
			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When UpdateBuildSearchTaskState returns an error", func() {
			auditor := auditortest.NewErroring(instance.UpdateImportTasksAction, audit.Unsuccessful)
			body := strings.NewReader(`{"build_search_indexes":[{"dimension_name": "geography", "state":"completed"}]}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
				UpdateBuildSearchTaskStateFunc: func(id string, dimension string, state string) error {
					return errors.New("error")
				},
			}
			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Unsuccessful, auditParams),
				)
			})
		})
	})
}

func Test_UpdateImportTask_AuditSuccessfulError(t *testing.T) {
	t.Parallel()
	Convey("Given audit action successful returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.UpdateImportTasksAction, audit.Successful)

		Convey("When update import task is called", func() {
			body := strings.NewReader(`{"import_observations":{"state":"completed"}}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
				UpdateImportObservationsTaskStateFunc: func(id string, state string) error {
					return nil
				},
			}

			datasetPermissions := mocks.NewAuthHandlerMock()
			permissions := mocks.NewAuthHandlerMock()

			datasetAPI := getAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, datasetPermissions, permissions)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.UpdateImportTasksAction, audit.Successful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})
}
