package instance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/smartystreets/goconvey/convey"
)

const (
	testIfMatch = "testIfMatch"
	testETag    = "testETag"
	AnyETag     = "*"
)

func Test_UpdateDimensionReturnsOk(t *testing.T) {
	t.Parallel()

	bodyStr := `{"label":"ages", "description": "A range of ages between 18 and 60"}`
	expectedUpdate := &models.Instance{
		Dimensions: []models.Dimension{
			{
				Label:       "ages",
				Description: "A range of ages between 18 and 60",
				ID:          "age",
				Name:        "age",
			},
		},
	}

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
			return &models.Instance{State: models.EditionConfirmedState,
				InstanceID: "123",
				Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
		}
		mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
			return testETag, nil
		}

		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When a PUT request to update an instance dimension is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
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
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].UpdatedInstance, convey.ShouldResemble, expectedUpdate)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a PUT request to update an instance dimension is made, without an If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
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
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].UpdatedInstance, convey.ShouldResemble, expectedUpdate)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateDimensionReturnsInternalError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update a dimension on an instance resource", t, func() {
		convey.Convey("When service is unable to connect to datastore", func() {
			convey.Convey("Then return status internal error (500)", func() {
				body := strings.NewReader(`{"label":"ages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				}
				mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
					return testETag, nil
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
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the current instance state is invalid", func() {
			convey.Convey("Then return status internal error (500)", func() {
				body := strings.NewReader(`{"label":"ages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				r.Header.Set("If-Match", testIfMatch)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: "gobbly gook"}, nil
				}
				mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
					return testETag, nil
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
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the resource has a state of published", func() {
			convey.Convey("Then return status forbidden (403)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", nil)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.PublishedState}, nil
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
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the instance does not exist", func() {
			convey.Convey("Then return status not found (404) with message 'instance not found'", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", nil)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
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
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the dimension does not exist against instance", func() {
			convey.Convey("Then return status not found (404) with message 'dimension not found'", func() {
				body := strings.NewReader(`{"label":"notages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/notage", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState,
						InstanceID: "123",
						Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
				}
				mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
					return testETag, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDimensionNotFound.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the request body is invalid json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader("{")
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{State: models.CompletedState}, nil
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When the provided If-Match header value does not match the instance eTag", func() {
			convey.Convey("Then return status conflict (409)", func() {
				body := strings.NewReader(`{"label":"ages", "description": "A range of ages between 18 and 60"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				r.Header.Set("If-Match", "wrong")
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
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

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, "wrong")

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}
