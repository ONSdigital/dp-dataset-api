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
					GetInstancesFunc: func([]string) (*models.InstanceResults, error) {
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
					GetInstancesFunc: func(filterString []string) (*models.InstanceResults, error) {
						result = filterString
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
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, common.Params{"query": "completed"}),
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
					GetInstancesFunc: func(filterString []string) (*models.InstanceResults, error) {
						result = filterString
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
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Successful, common.Params{"query": "completed,edition-confirmed"}),
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
					GetInstancesFunc: func([]string) (*models.InstanceResults, error) {
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
					auditortest.NewExpectation(instance.GetInstancesAction, audit.Unsuccessful, common.Params{"query": "foo"}),
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
				GetInstancesFunc: func([]string) (*models.InstanceResults, error) {
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
				GetInstancesFunc: func([]string) (*models.InstanceResults, error) {
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
						return &models.Instance{State: models.CreatedState}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Successful, auditParams},
				)
			})
		})

		Convey("When the requested state change is to 'edition-confirmed'", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Job: &models.IDLink{
							ID:   "7654",
							HRef: "job-link",
						},
						Dataset: &models.IDLink{
							ID:   "4567",
							HRef: "dataset-link",
						},
						Self: &models.IDLink{
							HRef: "self-link",
						},
					},
					State: models.CompletedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					GetEditionFunc: func(datasetID string, edition string, state string) (*models.EditionUpdate, error) {
						return nil, errs.ErrEditionNotFound
					},
					UpsertEditionFunc: func(datasetID, edition string, editionDoc *models.EditionUpdate) error {
						return nil
					},
					GetNextVersionFunc: func(string, string) (int, error) {
						return 1, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
					AddVersionDetailsToInstanceFunc: func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.CreateEditionAction, audit.Attempted, common.Params{"instance_id": "123", "dataset_id": "4567", "edition": "2017"}},
					auditortest.Expected{instance.CreateEditionAction, audit.Successful, common.Params{"instance_id": "123", "dataset_id": "4567", "edition": "2017"}},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Successful, auditParams},
				)
			})
		})
	})
}

func Test_UpdateInstanceReturnsError(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
	editionAuditParams := common.Params{"instance_id": "123", "dataset_id": "4567", "edition": "2017"}

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

		Convey(`When request updates state to 'edition-confirmed'
						but fails to update instance with version details`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Job: &models.IDLink{
							ID:   "7654",
							HRef: "job-link",
						},
						Dataset: &models.IDLink{
							ID:   "4567",
							HRef: "dataset-link",
						},
						Self: &models.IDLink{
							HRef: "self-link",
						},
					},
					State: models.CompletedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					GetEditionFunc: func(datasetID string, edition string, state string) (*models.EditionUpdate, error) {
						return nil, errs.ErrEditionNotFound
					},
					UpsertEditionFunc: func(datasetID, edition string, editionDoc *models.EditionUpdate) error {
						return nil
					},
					GetNextVersionFunc: func(string, string) (int, error) {
						return 1, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
					AddVersionDetailsToInstanceFunc: func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
						return errors.New("boom")
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.CreateEditionAction, audit.Attempted, editionAuditParams},
					auditortest.Expected{instance.CreateEditionAction, audit.Successful, editionAuditParams},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey(`When request updates instance from a state 'edition-confirmed' to 'completed'`, func() {
			Convey("Then return status forbidden (403)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Dataset: &models.IDLink{
							ID: "4567",
						},
					},
					State: models.EditionConfirmedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrExpectedResourceStateOfSubmitted.Error())

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

func Test_UpdateInstance_AuditFailure(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
	editionAuditParams := common.Params{"instance_id": "123", "dataset_id": "4567", "edition": "2017"}

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
						Dataset: &models.IDLink{
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
						Dataset: &models.IDLink{
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

		Convey(`When the requested state change is to 'edition-confirmed' and attempt to audit request to create edition fails`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Job: &models.IDLink{
							ID:   "7654",
							HRef: "job-link",
						},
						Dataset: &models.IDLink{
							ID:   "4567",
							HRef: "dataset-link",
						},
						Self: &models.IDLink{
							HRef: "self-link",
						},
					},
					State: models.CompletedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					GetEditionFunc: func(datasetID string, edition string, state string) (*models.EditionUpdate, error) {
						return nil, errs.ErrEditionNotFound
					},
				}

				auditor := auditortest.NewErroring(instance.CreateEditionAction, audit.Attempted)
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.CreateEditionAction, audit.Attempted, editionAuditParams},
					auditortest.Expected{instance.CreateEditionAction, audit.Unsuccessful, editionAuditParams},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey(`When the requested state changes to 'associated' and the edition is updated
			but unable to update instance and then the auditor attempts an unsuccessful message and fails`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Job: &models.IDLink{
							ID:   "7654",
							HRef: "job-link",
						},
						Dataset: &models.IDLink{
							ID:   "4567",
							HRef: "dataset-link",
						},
						Self: &models.IDLink{
							HRef: "self-link",
						},
					},
					State: models.CompletedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					GetEditionFunc: func(datasetID string, edition string, state string) (*models.EditionUpdate, error) {
						return &models.EditionUpdate{
							Current: &models.Edition{
								State: models.PublishedState,
							},
							Next: &models.Edition{
								Links: &models.EditionUpdateLinks{
									Dataset: &models.LinkObject{
										HRef: "/dataset/test/href",
										ID:   "cpih01",
									},
									LatestVersion: &models.LinkObject{
										ID: "2",
									},
									Self: &models.LinkObject{
										HRef: "/edition/test/href",
									},
								},
								State: models.EditionConfirmedState,
							},
						}, nil
					},
					UpsertEditionFunc: func(datasetID, edition string, editionDoc *models.EditionUpdate) error {
						return nil
					},
					GetNextVersionFunc: func(string, string) (int, error) {
						return 1, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return errs.ErrInternalServer
					},
					AddVersionDetailsToInstanceFunc: func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
						return nil
					},
				}

				auditor := auditortest.NewErroring(instance.UpdateInstanceAction, audit.Unsuccessful)
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateEditionAction, audit.Attempted, editionAuditParams},
					auditortest.Expected{instance.UpdateEditionAction, audit.Successful, editionAuditParams},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Unsuccessful, auditParams},
				)
			})
		})

		Convey(`When the requested state changes to 'associated' and the edition
			 is updated, yet the auditor fails is unsuccessful in writing success message`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Job: &models.IDLink{
							ID:   "7654",
							HRef: "job-link",
						},
						Dataset: &models.IDLink{
							ID:   "4567",
							HRef: "dataset-link",
						},
						Self: &models.IDLink{
							HRef: "self-link",
						},
					},
					State: models.CompletedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					GetEditionFunc: func(datasetID string, edition string, state string) (*models.EditionUpdate, error) {
						return &models.EditionUpdate{
							Current: &models.Edition{
								State: models.PublishedState,
							},
							Next: &models.Edition{
								Links: &models.EditionUpdateLinks{
									Dataset: &models.LinkObject{
										HRef: "/dataset/test/href",
										ID:   "cpih01",
									},
									LatestVersion: &models.LinkObject{
										ID: "2",
									},
									Self: &models.LinkObject{
										HRef: "/edition/test/href",
									},
								},
								State: models.EditionConfirmedState,
							},
						}, nil
					},
					UpsertEditionFunc: func(datasetID, edition string, editionDoc *models.EditionUpdate) error {
						return nil
					},
					GetNextVersionFunc: func(string, string) (int, error) {
						return 1, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
					AddVersionDetailsToInstanceFunc: func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
						return nil
					},
				}

				auditor := auditortest.NewErroring(instance.UpdateInstanceAction, audit.Unsuccessful)
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.Expected{instance.UpdateInstanceAction, audit.Attempted, auditParamsWithCallerIdentity},
					auditortest.Expected{instance.UpdateEditionAction, audit.Attempted, editionAuditParams},
					auditortest.Expected{instance.UpdateEditionAction, audit.Successful, editionAuditParams},
					auditortest.Expected{instance.UpdateInstanceAction, audit.Successful, auditParams},
				)
			})
		})
	})
}

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInsertedObservationsInvalidSyntax.Error())

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

			auditor := auditortest.NewErroring(instance.UpdateInsertedObservationsAction, audit.Attempted)
			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

			auditor := auditortest.NewErroring(instance.UpdateInsertedObservationsAction, audit.Unsuccessful)
			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

			auditor := auditortest.NewErroring(instance.UpdateInsertedObservationsAction, audit.Unsuccessful)
			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 200 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid import observation task, must include state")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value for import observations: notvalid")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - request body does not contain any import tasks")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing hierarchy task")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value: notvalid")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, "geography hierarchy import task does not exist")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - request body does not contain any import tasks")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing search index task")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value: notvalid")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, "geography search index import task does not exist")

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

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
			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
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
							Dimensions: []models.CodeList{{Name: "age", ID: "age"}}}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
							Dimensions: []models.CodeList{{Name: "age", ID: "age"}}}, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
						Dimensions: []models.CodeList{{Name: "age", ID: "age"}}}, nil
				},
				UpdateInstanceFunc: func(ctx context.Context, id string, i *models.Instance) error {
					return nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
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
