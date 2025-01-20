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
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/smartystreets/goconvey/convey"
)

const testLockID = "testLock"

func Test_UpdateInstanceToEditionConfirmedReturnsOk(t *testing.T) {
	convey.Convey("Given a dataset API with auth and a successful store mock with a 'completed' generic instance", t, func() {
		i := completedInstance()

		mockedDataStore, isLocked := storeMockEditionCompleteWithLock(i, true)
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When the requested state change is to 'edition-confirmed'", func() {
			body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected permission required functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
			})

			convey.Convey("Then the expected mongoDB functions are called", func() {
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 3)
				convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 1)
			})

			convey.Convey("Then the dp-graph function is called", func() {
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 1)
			})

			convey.Convey("Then the mongoDB instance lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})

	convey.Convey("Given a dataset API with auth and a successful store mock with a 'completed' cantabular_blob instance", t, func() {
		i := completedInstance()
		i.Type = models.CantabularBlob.String()

		mockedDataStore, isLocked := storeMockEditionCompleteWithLock(i, true)
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When the requested state change is to 'edition-confirmed'", func() {
			body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected permission required functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
			})

			convey.Convey("Then the expected mongoDB functions are called", func() {
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 3)
				convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 1)
			})

			convey.Convey("Then the dp-graph function is not called", func() {
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("Then the mongoDB instance lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateInstanceToEditionConfirmedReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update state of an instance resource is made", t, func() {
		convey.Convey(`When request updates state to 'edition-confirmed'
        but fails to update instance with version details`, func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTestData := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Job: &models.LinkObject{
							ID:   "7654",
							HRef: "job-link",
						},
						Dataset: &models.LinkObject{
							ID:   "4567",
							HRef: "dataset-link",
						},
						Self: &models.LinkObject{
							HRef: "self-link",
						},
					},
					State: models.CompletedState,
				}

				mockedDataStore, isLocked := storeMockEditionCompleteWithLock(currentInstanceTestData, true)
				mockedDataStore.AddVersionDetailsToInstanceFunc = func(_ context.Context, _ string, _ string, _ string, _ int) error {
					convey.So(*isLocked, convey.ShouldBeTrue)
					return errors.New("boom")
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey(`When request updates instance from a state 'edition-confirmed' to 'completed'`, func() {
			convey.Convey("Then return status forbidden (403)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTestData := &models.Instance{
					Edition: "2017",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							ID: "4567",
						},
					},
					State: models.EditionConfirmedState,
				}

				mockedDataStore, isLocked := storeMockWithLock(currentInstanceTestData, true)
				mockedDataStore.UpdateInstanceFunc = func(_ context.Context, _ *models.Instance, _ *models.Instance, _ string) (string, error) {
					convey.So(isLocked, convey.ShouldBeTrue)
					return testETag, nil
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrExpectedResourceStateOfSubmitted.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 2)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	convey.So(mockedDataStore.AcquireInstanceLockCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, convey.ShouldEqual, expectedInstanceID)
	convey.So(mockedDataStore.UnlockInstanceCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.UnlockInstanceCalls()[0].LockID, convey.ShouldEqual, testLockID)
}

func storeMockEditionCompleteWithLock(instance *models.Instance, expectFirstGetUnlocked bool) (mockedDataStore *storetest.StorerMock, isLocked *bool) {
	mockedDataStore, isLocked = storeMockWithLock(instance, expectFirstGetUnlocked)
	mockedDataStore.GetEditionFunc = func(_ context.Context, _ string, _ string, _ string) (*models.EditionUpdate, error) {
		convey.So(*isLocked, convey.ShouldBeTrue)
		return nil, errs.ErrEditionNotFound
	}
	mockedDataStore.UpsertEditionFunc = func(_ context.Context, _, _ string, _ *models.EditionUpdate) error {
		convey.So(*isLocked, convey.ShouldBeTrue)
		return nil
	}
	mockedDataStore.GetNextVersionFunc = func(context.Context, string, string) (int, error) {
		convey.So(*isLocked, convey.ShouldBeTrue)
		return 1, nil
	}
	mockedDataStore.UpdateInstanceFunc = func(_ context.Context, _ *models.Instance, _ *models.Instance, _ string) (string, error) {
		convey.So(*isLocked, convey.ShouldBeTrue)
		return testETag, nil
	}
	mockedDataStore.AddVersionDetailsToInstanceFunc = func(_ context.Context, _ string, _ string, _ string, _ int) error {
		convey.So(*isLocked, convey.ShouldBeTrue)
		return nil
	}
	return mockedDataStore, isLocked
}

func completedInstance() *models.Instance {
	return &models.Instance{
		Edition: "2017",
		Links: &models.InstanceLinks{
			Job: &models.LinkObject{
				ID:   "7654",
				HRef: "job-link",
			},
			Dataset: &models.LinkObject{
				ID:   "4567",
				HRef: "dataset-link",
			},
			Self: &models.LinkObject{
				HRef: "self-link",
			},
		},
		State: models.CompletedState,
	}
}

func storeMockWithLock(instance *models.Instance, expectFirstGetUnlocked bool) (mockedDataStore *storetest.StorerMock, isLockedPointer *bool) {
	isLocked := false
	numGetCall := 0
	mockedDataStore = &storetest.StorerMock{
		AcquireInstanceLockFunc: func(_ context.Context, _ string) (string, error) {
			isLocked = true
			return testLockID, nil
		},
		UnlockInstanceFunc: func(_ context.Context, _ string) {
			isLocked = false
		},
		GetInstanceFunc: func(_ context.Context, _ string, _ string) (*models.Instance, error) {
			if expectFirstGetUnlocked {
				if numGetCall > 0 {
					convey.So(isLocked, convey.ShouldBeTrue)
				} else {
					convey.So(isLocked, convey.ShouldBeFalse)
				}
			}
			numGetCall++
			return instance, nil
		},
	}
	return mockedDataStore, &isLocked
}
