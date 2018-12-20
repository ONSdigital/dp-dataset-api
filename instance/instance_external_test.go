package instance_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dataset-api/api"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	"github.com/ONSdigital/go-ns/common"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const host = "http://localhost:8080"

var errAudit = errors.New("auditing error")

func createRequestWithToken(method, url string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	ctx := r.Context()
	ctx = common.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func Test_GetInstancesReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Given a GET request to retrieve a list of instance resources is made", t, func() {
		Convey("When the request is authorised", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func([]string, []string) (*models.InstanceResults, error) {
						return &models.InstanceResults{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, nil),
				)
			})
		})

		Convey("When the request includes a filter by state of 'completed'", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances?state=completed", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				var result []string

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func(state []string, dataset []string) (*models.InstanceResults, error) {
						result = state
						return &models.InstanceResults{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(result, ShouldResemble, []string{models.CompletedState})
				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, common.Params{"state_query": "completed"}),
				)
			})
		})

		Convey("When the request includes a filter by dataset of 'test'", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances?dataset=test", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				var result []string

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func(state []string, dataset []string) (*models.InstanceResults, error) {
						result = dataset
						return &models.InstanceResults{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(result, ShouldResemble, []string{"test"})
				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, common.Params{"dataset_query": "test"}),
				)
			})
		})

		Convey("When the request includes a filter by state of multiple values 'completed,edition-confirmed'", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances?state=completed,edition-confirmed", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				var result []string

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func(state []string, dataset []string) (*models.InstanceResults, error) {
						result = state
						return &models.InstanceResults{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(result, ShouldResemble, []string{models.CompletedState, models.EditionConfirmedState})
				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, common.Params{"state_query": "completed,edition-confirmed"}),
				)
			})
		})

		Convey("When the request includes a filter by state of 'completed' and dataset 'test'", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances?state=completed&dataset=test", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				var result []string

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func(state []string, dataset []string) (*models.InstanceResults, error) {
						result = append(result, state...)
						result = append(result, dataset...)
						return &models.InstanceResults{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(result, ShouldResemble, []string{models.CompletedState, "test"})
				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, common.Params{"state_query": "completed", "dataset_query": "test"}),
				)
			})
		})
	})
}

func Test_GetInstancesReturnsError(t *testing.T) {
	t.Parallel()
	Convey("Given a GET request to retrieve a list of instance resources is made", t, func() {
		Convey("When the service is unable to connect to the datastore", func() {
			Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func([]string, []string) (*models.InstanceResults, error) {
						return nil, errs.ErrInternalServer
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Unsuccessful, nil),
				)
			})
		})

		Convey("When the request contains an invalid state to filter on", func() {
			Convey("Then return status bad request (400)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances?state=foo", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid filter state values: [foo]")

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Unsuccessful, common.Params{"state_query": "foo"}),
				)
			})
		})
	})
}

func Test_GetInstancesAuditErrors(t *testing.T) {
	t.Parallel()
	Convey("Given audit action attempted returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.GetInstancesAction, audit.Attempted)

		Convey("When get instances is called", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
				)
			})
		})
	})

	Convey("Given audit action unsuccessful returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.GetInstancesAction, audit.Unsuccessful)

		Convey("When get instances return an error", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func([]string, []string) (*models.InstanceResults, error) {
					return nil, errs.ErrInternalServer
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Unsuccessful, nil),
				)
			})
		})
	})

	Convey("Given audit action successful returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.GetInstancesAction, audit.Successful)

		Convey("When get instances is called", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func([]string, []string) (*models.InstanceResults, error) {
					return &models.InstanceResults{}, nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk"}),
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, nil),
				)
			})
		})
	})
}

func Test_GetInstanceReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Given a GET request to retrieve an instance resource is made", t, func() {
		Convey("When the request is authorised", func() {
			Convey("Then return status ok (200)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string) (*models.Instance, error) {
						return &models.Instance{State: models.CreatedState}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}},
					auditortest.Expected{instance.GetInstanceAction, audit.Successful, common.Params{"instance_id": "123"}},
				)
			})
		})
	})
}

func Test_GetInstanceReturnsError(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

	t.Parallel()
	Convey("Given a GET request to retrieve an instance resource is made", t, func() {
		Convey("When the service is unable to connect to the datastore", func() {
			Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.GetInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey("When the current instance state is invalid", func() {
			Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string) (*models.Instance, error) {
						return &models.Instance{State: "gobbledygook"}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.GetInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey("When the instance resource does not exist", func() {
			Convey("Then return status not found (404)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.GetInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})
	})
}

func Test_GetInstanceAuditErrors(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

	t.Parallel()
	Convey("Given audit action 'attempted' fails", t, func() {
		auditor := auditortest.NewErroring(instance.GetInstanceAction, audit.Attempted)

		Convey("When a GET request is made to get an instance resource", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(ID string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
				)
			})
		})
	})

	Convey("Given audit action 'unsuccessful' fails", t, func() {
		auditor := auditortest.NewErroring(instance.GetInstanceAction, audit.Unsuccessful)

		Convey("When a GET request is made to get an instance resource", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(ID string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.GetInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})
	})

	Convey("Given audit action 'successful' fails", t, func() {
		auditor := auditortest.NewErroring(instance.GetInstanceAction, audit.Successful)

		Convey("When a GET request is made to get an instance resource", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstanceFunc: func(ID string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.GetInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.GetInstanceAction, audit.Successful, auditParams},
				)
			})
		})
	})
}

type expectedPostInstanceAuditObject struct {
	Action      string
	ContainsKey string
	Result      string
}

// function specifically used for POST instance as instance_id cannot be
// determined due to the nature of the handler method creating it's value
func checkAuditRecord(auditor auditortest.MockAuditor, expected []expectedPostInstanceAuditObject) {
	So(len(auditor.RecordCalls()), ShouldEqual, len(expected))
	for i, _ := range expected {
		// Instance_id is created with a new uuid every time the Test_ is run and
		// hence cannot use the AssertRecordCalls helper method
		So(auditor.RecordCalls()[i].Action, ShouldEqual, expected[i].Action)
		So(auditor.RecordCalls()[i].Result, ShouldEqual, expected[i].Result)
		if expected[i].ContainsKey != "" {
			So(auditor.RecordCalls()[i].Params, ShouldNotBeNil)
			So(auditor.RecordCalls()[i].Params, ShouldContainKey, expected[i].ContainsKey)
		}
	}
}

func Test_AddInstanceReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("Given a POST request to create an instance resource", t, func() {
		Convey("When the request is authorised", func() {
			Convey("Then return status created (201)", func() {
				body := strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
						return &models.Instance{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusCreated)
				So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Successful, ContainsKey: "",
					},
				})
			})
		})
	})
}

func Test_AddInstanceReturnsError(t *testing.T) {
	t.Parallel()
	Convey("Given a POST request to create an instance resources", t, func() {
		Convey("When the service is unable to connect to the datastore", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"links": {"job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Unsuccessful, ContainsKey: "",
					},
				})
			})
		})

		Convey("When the request contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
						return &models.Instance{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
				So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 0)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Unsuccessful, ContainsKey: "",
					},
				})
			})
		})

		Convey("When the request contains empty json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
						return &models.Instance{}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrMissingJobProperties.Error())
				So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 0)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Unsuccessful, ContainsKey: "",
					},
				})
			})
		})
	})
}

func Test_AddInstanceAuditErrors(t *testing.T) {
	t.Parallel()
	Convey("Given audit action 'attempted' fails", t, func() {
		auditor := auditortest.NewErroring(instance.AddInstanceAction, audit.Attempted)

		Convey("When a POST request is made to create an instance resource", func() {
			body := strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
				})
			})
		})
	})

	Convey("Given audit action 'unsuccessful' fails", t, func() {
		auditor := auditortest.NewErroring(instance.AddInstanceAction, audit.Unsuccessful)

		Convey("When a POST request is made to create an instance resource", func() {
			body := strings.NewReader(`{"links": {"job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns internal server error (500)", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Unsuccessful, ContainsKey: "",
					},
				})
			})
		})
	})

	Convey("Given audit action 'successful' fails", t, func() {
		auditor := auditortest.NewErroring(instance.AddInstanceAction, audit.Successful)

		Convey("When a POST request is made to create an instance resource", func() {
			body := strings.NewReader(`{"links": {"job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
					return &models.Instance{}, nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then response returns status created (201)", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)

				checkAuditRecord(*auditor, []expectedPostInstanceAuditObject{
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Attempted, ContainsKey: "caller_identity",
					},
					expectedPostInstanceAuditObject{
						Action: instance.AddInstanceAction, Result: audit.Successful, ContainsKey: "",
					},
				})
			})
		})
	})
}

func Test_UpdateInstanceReturnsOk(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

	t.Parallel()
	Convey("Given a PUT request to update state of an instance resource is made", t, func() {
		Convey("When the requested state change is to 'submitted'", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"state":"submitted"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{
							Links: &models.InstanceLinks{
								Dataset: &models.LinkObject{
									ID:   "234",
									HRef: "example.com/234",
								},
								Self: &models.LinkObject{
									ID:   "123",
									HRef: "example.com/123",
								},
							},
							State: models.CreatedState,
						}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Successful, auditParams},
				)
			})
		})

	})
}

func Test_UpdateInstanceReturnsError(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

	t.Parallel()
	Convey("Given a PUT request to update state of an instance resource is made", t, func() {
		Convey("When the service is unable to connect to the datastore", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"created"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey("When the current instance state is invalid", func() {
			Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", strings.NewReader(`{"state":"completed", "edition": "2017"}`))
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: "gobbledygook"}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, common.Params{"instance_id": "123", "instance_state": "gobbledygook"}},
				)
			})
		})

		Convey("When the json body is invalid", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"state":`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: "completed"}, nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey("When the json body contains fields that are not allowed to be updated", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"links": { "dataset": { "href": "silly-site"}, "version": { "href": "sillier-site"}}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return &models.Instance{State: "completed"}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unable to update instance contains invalid fields: [instance.Links.Dataset instance.Links.Version]")

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey("When the instance does not exist", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})
	})
}

func Test_UpdateInstance_AuditFailure(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
	//editionAuditParams := common.Params{"instance_id": "123", "dataset_id": "4567", "edition": "2017"}

	t.Parallel()
	Convey("Given a PUT request to update state of an instance resource is made", t, func() {
		Convey(`When the auditor attempts to audit request attempt fails`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return nil, nil
					},
				}

				auditor := auditortest.NewErroring(instance.UpdateInstanceAction, audit.Attempted)
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
				)
			})
		})

		Convey(`When the instance resource is already published and hence request is
	 forbidden and so auditor attempts to audit unsuccessful request but it fails to do so`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							ID: "4567",
						},
					},
					State: models.PublishedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.NewErroring(instance.UpdateInstanceAction, audit.Unsuccessful)
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, common.Params{"instance_id": "123", "instance_state": models.PublishedState}},
				)
			})
		})

		Convey(`When the request state change is invalid and the attempt to audit unsuccessful fails`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							ID: "4567",
						},
					},
					State: models.AssociatedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.NewErroring(instance.UpdateInstanceAction, audit.Unsuccessful)
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})
	})
}

var urlBuilder = url.NewBuilder("localhost:20000")

func getAPIWithMockedDatastore(mockedDataStore store.Storer, mockedGeneratedDownloads api.DownloadsGenerator, mockAuditor api.Auditor, mockedObservationStore api.ObservationStore) *api.DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = "dataset"
	cfg.DatasetAPIURL = "http://localhost:22000"
	cfg.EnablePrivateEnpoints = true
	cfg.HealthCheckTimeout = 2 * time.Second

	return api.Routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, mockAuditor, mockedObservationStore)
}
