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
	"github.com/smartystreets/goconvey/convey"
)

func Test_InsertedObservationsReturnsOk(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
			State:      models.EditionConfirmedState,
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return &models.Instance{State: models.EditionConfirmedState}, nil
		}
		mockedDataStore.UpdateObservationInsertedFunc = func(context.Context, *models.Instance, int64, string) (string, error) {
			return testETag, nil
		}
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When a PUT request to update the inserted observations for an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateObservationInsertedCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.UpdateObservationInsertedCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateObservationInsertedCalls()[0].ObservationInserted, convey.ShouldEqual, 200)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a PUT request to update the inserted observations for an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateObservationInsertedCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.UpdateObservationInsertedCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateObservationInsertedCalls()[0].ObservationInserted, convey.ShouldEqual, 200)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_InsertedObservationsReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with inserted observations", t, func() {
		convey.Convey("When the service is unable to connect to the datastore", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateObservationInsertedCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the instance no longer exists after validating instance state", func() {
			convey.Convey("Then return status not found (404)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateObservationInsertedFunc = func(context.Context, *models.Instance, int64, string) (string, error) {
					return "", errs.ErrInstanceNotFound
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateObservationInsertedCalls()), convey.ShouldEqual, 1)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request parameter 'inserted_observations' is not an integer value", func() {
			convey.Convey("Then return status bad request (400)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/aa12a", nil)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.SubmittedState}, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInsertedObservationsInvalidSyntax.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateObservationInsertedCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the provided If-Match header value does not match the instance eTag", func() {
			convey.Convey("Then return status conflict (409)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
				r.Header.Set("If-Match", "wrong")
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInstanceConflict
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceConflict.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, "wrong")

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateImportObservationsReturnsOk(t *testing.T) {
	t.Parallel()

	bodyStr := `{"import_observations":{"state":"completed"}}`

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
			State:      models.EditionConfirmedState,
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(_ context.Context, ID string, _ string) (*models.Instance, error) {
			return &models.Instance{
				InstanceID: ID,
				State:      models.CreatedState,
			}, nil
		}
		mockedDataStore.UpdateImportObservationsTaskStateFunc = func(context.Context, *models.Instance, string, string) (string, error) {
			return testETag, nil
		}
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When a PUT request to update the import_observations value for an import task of an instance resource is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].State, convey.ShouldEqual, models.CompletedState)
			})

			convey.Convey("Then the handle was executed using a lock for the expected instance ID, and the lock was released", func() {
				convey.So(*isLocked, convey.ShouldBeFalse)
				validateLock(mockedDataStore, "123")
			})
		})

		convey.Convey("When a PUT request to retrieve an instance resource is made, without an If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateImportObservationsTaskStateCalls()[0].State, convey.ShouldEqual, models.CompletedState)
			})

			convey.Convey("Then the handle was executed using a lock for the expected instance ID, and the lock was released", func() {
				convey.So(*isLocked, convey.ShouldBeFalse)
				validateLock(mockedDataStore, "123")
			})
		})
	})
}

func Test_UpdateImportTaskRetrunsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with import task", t, func() {
		convey.Convey("When the service is unable to connect to the datastore", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the instance resource does not exist", func() {
			convey.Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInstanceNotFound
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the instance resource is already published", func() {
			convey.Convey("Then return status forbidden (403)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.PublishedState}, nil
				}
				mockedDataStore.UpdateInstanceFunc = func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrResourcePublished.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateImportObservationsReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with import observations", t, func() {
		convey.Convey("When the request body contains invalid json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(context.Context, *models.Instance, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "unexpected end of JSON input")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body is missing mandatory field, 'state'", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"import_observations":{}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(context.Context, *models.Instance, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - invalid import observation task, must include state")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body contains an invalid 'state' value", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"notvalid"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(context.Context, *models.Instance, string, string) (string, error) {
					return testETag, nil
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - invalid task state value for import observations: notvalid")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the service loses connection to datastore whilst updating observations", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"import_observations":{"state":"completed"}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateImportObservationsTaskStateFunc = func(context.Context, *models.Instance, string, string) (string, error) {
					return "", errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 1)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_BuildHierarchyTaskReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with import task 'build hierarchies'", t, func() {
		convey.Convey("When the request body contains invalid json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "unexpected end of JSON input")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body contains empty json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - request body does not contain any import tasks")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body contains empty 'build_hierarchies' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - missing hierarchy task")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body is missing 'dimension_name' from 'build_hierarchies' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body is missing 'state' from 'build_hierarchies' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the import task has an invalid 'state' value inside the 'build_hierarchies' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"notvalid", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - invalid task state value: notvalid")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the import task has the incorrect 'dimension_name' value in the 'build_hierarchies' object", func() {
			convey.Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return "", errors.New("not found")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "geography hierarchy import task does not exist")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When service loses connection to datastore while updating resource", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return "", errors.New("internal error")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_BuildHierarchyTaskReturnsOk(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with import task 'build hierarchies'", t, func() {
		convey.Convey("When the request body is valid", func() {
			convey.Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"build_hierarchies":[{"state":"completed", "dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState}, nil
				}
				mockedDataStore.UpdateBuildHierarchyTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 1)

				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateBuildSearchIndexTask_Failure(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with import task 'build search indexes'", t, func() {
		convey.Convey("When the request body contains invalid json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "unexpected end of JSON input")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body contains empty json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - request body does not contain any import tasks")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body contains empty 'build_search_indexes' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - missing search index task")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body is missing 'dimension_name' from 'build_search_indexes' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - missing mandatory fields: [dimension_name]")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body is missing 'state' from 'build_search_indexes' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"dimension_name":"geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - missing mandatory fields: [state]")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the import task has an invalid 'state' value inside the 'build_search_indexes' object", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"notvalid", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - invalid task state value: notvalid")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the import task has the incorrect 'dimension_name' value in the 'build_search_indexes' object", func() {
			convey.Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return "", errors.New("not found")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "geography search index import task does not exist")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 1)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When service loses connection to datastore while updating resource", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return "", errors.New("internal error")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 1)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateImportTask_UpdateBuildSearchIndexReturnsOk(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update an instance resource with import task 'build_search_indexes'", t, func() {
		convey.Convey("When the request body is valid", func() {
			convey.Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"build_search_indexes":[{"state":"completed", "dimension_name": "geography"}]}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/import_tasks", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
					State:      models.EditionConfirmedState,
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CreatedState}, nil
				}
				mockedDataStore.UpdateBuildSearchTaskStateFunc = func(context.Context, *models.Instance, string, string, string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateImportObservationsTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildHierarchyTaskStateCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateBuildSearchTaskStateCalls()), convey.ShouldEqual, 1)

				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}
