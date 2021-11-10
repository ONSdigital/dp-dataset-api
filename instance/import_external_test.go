package instance_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_InsertedObservationsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
			State:      models.EditionConfirmedState,
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			return &models.Instance{State: models.EditionConfirmedState}, nil
		}
		mockedDataStore.UpdateObservationInsertedFunc = func(id *models.Instance, ob int64, eTagSelector string) (string, error) {
			return testETag, nil
		}
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When a PUT request to update the inserted observations for an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.UpdateObservationInsertedCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.UpdateObservationInsertedCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.UpdateObservationInsertedCalls()[0].ObservationInserted, ShouldEqual, 200)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a PUT request to update the inserted observations for an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.UpdateObservationInsertedCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.UpdateObservationInsertedCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.UpdateObservationInsertedCalls()[0].ObservationInserted, ShouldEqual, 200)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
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

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the instance no longer exists after validating instance state", func() {
			Convey("Then return status not found (404)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateObservationInsertedFunc = func(id *models.Instance, ob int64, eTagSelector string) (string, error) {
					return "", errs.ErrInstanceNotFound
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request parameter 'inserted_observations' is not an integer value", func() {
			Convey("Then return status bad request (400)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/aa12a", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.SubmittedState}, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInsertedObservationsInvalidSyntax.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the provided If-Match header value does not match the instance eTag", func() {
			Convey("Then return status conflict (409)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				r.Header.Set("If-Match", "wrong")
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInstanceConflict
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusConflict)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceConflict.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, "wrong")

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateImportObservationsReturnsOk(t *testing.T) {
	t.Parallel()

	bodyStr := `{"import_observations":{"state":"completed"}}`

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
			State:      models.EditionConfirmedState,
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			return &models.Instance{
				InstanceID: ID,
				State:      models.CreatedState,
			}, nil
		}
		mockedDataStore.UpdateImportObservationsTaskStateFunc = func(currentInstance *models.Instance, state string, eTagSelector string) (string, error) {
			return testETag, nil
		}
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When a PUT request to update the import_observations value for an import task of an instance resource is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].CurrentInstance.InstanceID, ShouldEqual, "123")
				So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].State, ShouldEqual, models.CompletedState)
			})

			Convey("Then the handle was executed using a lock for the expected instance ID, and the lock was released", func() {
				So(*isLocked, ShouldBeFalse)
				validateLock(mockedDataStore, "123")
			})
		})

		Convey("When a PUT request to retrieve an instance resource is made, without an If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].CurrentInstance.InstanceID, ShouldEqual, "123")
				So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].State, ShouldEqual, models.CompletedState)
			})

			Convey("Then the handle was executed using a lock for the expected instance ID, and the lock was released", func() {
				So(*isLocked, ShouldBeFalse)
				validateLock(mockedDataStore, "123")
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

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the instance resource does not exist", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInstanceNotFound
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the instance resource is already published", func() {
			Convey("Then return status forbidden (403)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.PublishedState}, nil
				}
				mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateImportObservationsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import observations", t, func() {
		Convey("When the request body contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(currentInstance *models.Instance, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unexpected end of JSON input")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body is missing mandatory field, 'state'", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"import_observations":{}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(currentInstance *models.Instance, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid import observation task, must include state")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body contains an invalid 'state' value", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"notvalid"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(currentInstance *models.Instance, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value for import observations: notvalid")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the service loses connection to datastore whilst updating observations", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"completed"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(currentInstance *models.Instance, state string, eTagSelector string) (string, error) {
					return "", errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 1)

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_BuildHierarchyTaskReturnsError(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task 'build hierarchies'", t, func() {
		Convey("When the request body contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unexpected end of JSON input")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body contains empty json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - request body does not contain any import tasks")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body contains empty 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing hierarchy task")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body is missing 'dimension_name' from 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body is missing 'state' from 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the import task has an invalid 'state' value inside the 'build_hierarchies' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"notvalid", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value: notvalid")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the import task has the incorrect 'dimension_name' value in the 'build_hierarchies' object", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return "", errors.New("not found")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, "geography hierarchy import task does not exist")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When service loses connection to datastore while updating resource", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return "", errors.New("internal error")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
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

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 1)

				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateBuildSearchIndexTask_Failure(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update an instance resource with import task 'build search indexes'", t, func() {
		Convey("When the request body contains invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "unexpected end of JSON input")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body contains empty json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - request body does not contain any import tasks")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body contains empty 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing search index task")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body is missing 'dimension_name' from 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body is missing 'state' from 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the import task has an invalid 'state' value inside the 'build_search_indexes' object", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"notvalid", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, "bad request - invalid task state value: notvalid")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the import task has the incorrect 'dimension_name' value in the 'build_search_indexes' object", func() {
			Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return "", errors.New("not found")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, "geography search index import task does not exist")

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When service loses connection to datastore while updating resource", func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return "", errors.New("internal error")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				So(*isLocked, ShouldBeFalse)
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

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(currentInstance *models.Instance, dimension string, state string, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), ShouldEqual, 1)

				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}
