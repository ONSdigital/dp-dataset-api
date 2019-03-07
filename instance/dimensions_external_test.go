package instance_test

import (
	"context"
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

func Test_UpdateDimensionReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update a dimension on an instance resource", t, func() {
		Convey("When a valid request body is provided", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"label":"ages", "description": "A range of ages between 18 and 60"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState,
							InstanceID: "123",
							Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)

				auditParams := common.Params{"instance_id": "123", "dimension": "age", "instance_state": "edition-confirmed"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123", "dimension": "age"}),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Successful, auditParams),
				)
			})
		})
	})
}

func Test_UpdateDimensionReturnsInternalError(t *testing.T) {
	auditParams := common.Params{"instance_id": "123", "dimension": "age"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123", "dimension": "age"}

	t.Parallel()
	Convey("Given a PUT request to update a dimension on an instance resource", t, func() {
		Convey("When service is unable to connect to datastore", func() {
			Convey("Then return status internal error (500)", func() {
				body := strings.NewReader(`{"label":"ages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the current instance state is invalid", func() {
			Convey("Then return status internal error (500)", func() {
				body := strings.NewReader(`{"label":"ages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: "gobbly gook"}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditParamsWithState := common.Params{"instance_id": "123", "dimension": "age", "instance_state": "gobbly gook"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParamsWithState),
				)
			})
		})

		Convey("When the resource has a state of published", func() {
			Convey("Then return status forbidden (403)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.PublishedState}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditParamsWithState := common.Params{"instance_id": "123", "dimension": "age", "instance_state": models.PublishedState}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParamsWithState),
				)
			})
		})

		Convey("When the instance does not exist", func() {
			Convey("Then return status not found (404) with message 'instance not found'", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParams),
				)
			})
		})

		Convey("When the dimension does not exist against instance", func() {
			Convey("Then return status not found (404) with message 'dimension not found'", func() {
				body := strings.NewReader(`{"label":"notages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/notage", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.EditionConfirmedState,
							InstanceID: "123",
							Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionNotFound.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditParamsWithState := common.Params{"instance_id": "123", "dimension": "notage", "instance_state": models.EditionConfirmedState}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123", "dimension": "notage"}),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParamsWithState),
				)
			})
		})

		Convey("When the request body is invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader("{")
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: models.CompletedState}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditParamsWithState := common.Params{"instance_id": "123", "dimension": "age", "instance_state": models.CompletedState}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParamsWithState),
				)
			})
		})
	})
}

func Test_UpdateDimensionAuditErrors(t *testing.T) {
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123", "dimension": "age"}

	t.Parallel()
	Convey("Given audit action 'attempted' fails", t, func() {
		auditor := auditortest.NewErroring(instance.UpdateDimensionAction, audit.Attempted)

		Convey("When a PUT request is made to update dimension on an instance resource", func() {
			body := strings.NewReader(`{"label":"ages", "description": "A range of ages between 18 and 60"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
				)
			})
		})
	})

	Convey("Given audit action 'unsuccessful' fails", t, func() {
		auditor := auditortest.NewErroring(instance.UpdateDimensionAction, audit.Unsuccessful)

		Convey("When a PUT request is made to update dimension on an instance resource", func() {
			body := strings.NewReader("{")
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditParamsWithState := common.Params{"instance_id": "123", "dimension": "age", "instance_state": models.CreatedState}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Unsuccessful, auditParamsWithState),
				)
			})
		})
	})

	Convey("Given audit action 'successful' fails", t, func() {
		auditor := auditortest.NewErroring(instance.AddInstanceAction, audit.Successful)

		Convey("When a PUT request is made to update dimension on an instance resource", func() {
			body := strings.NewReader(`{"label":"ages", "description": "A range of ages between 18 and 60"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(id string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState,
						InstanceID: "123",
						Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
				},
				UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
					return nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor)
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns status ok (200)", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)

				auditParamsWithState := common.Params{"instance_id": "123", "dimension": "age", "instance_state": "edition-confirmed"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Attempted, auditParamsWithCallerIdentity),
					auditortest.NewExpectation(instance.UpdateDimensionAction, audit.Successful, auditParamsWithState),
				)
			})
		})
	})
}
