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
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

const testLockID = "testLock"

func Test_UpdateInstanceToEditionConfirmedReturnsOk(t *testing.T) {
	Convey("Given a dataset API with auth and a successful store mock with a 'completed' generic instance", t, func() {
		i := completedInstance()

		mockedDataStore, isLocked := storeMockEditionCompleteWithLock(i, true)
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When the requested state change is to 'edition-confirmed'", func() {
			body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected permission required functions are called", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
			})

			Convey("Then the expected mongoDB functions are called", func() {
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
			})

			Convey("Then the mongoDB instance lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})

	Convey("Given a dataset API with auth and a successful store mock with a 'completed' cantabular_blob instance", t, func() {
		i := completedInstance()
		i.Type = models.CantabularBlob.String()

		mockedDataStore, isLocked := storeMockEditionCompleteWithLock(i, true)
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When the requested state change is to 'edition-confirmed'", func() {
			body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected permission required functions are called", func() {
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
			})

			Convey("Then the expected mongoDB functions are called", func() {
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
			})

			Convey("Then the mongoDB instance lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateInstanceToEditionConfirmedReturnsError(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update state of an instance resource is made", t, func() {
		Convey(`When request updates state to 'edition-confirmed'
        but fails to update instance with version details`, func() {
			Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
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

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey(`When request updates instance from a state 'edition-confirmed' to 'completed'`, func() {
			Convey("Then return status forbidden (403)", func() {
				body := strings.NewReader(`{"state":"completed"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
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
					So(isLocked, ShouldBeTrue)
					return testETag, nil
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrExpectedResourceStateOfSubmitted.Error())
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	So(mockedDataStore.AcquireInstanceLockCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, ShouldEqual, expectedInstanceID)
	So(mockedDataStore.UnlockInstanceCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UnlockInstanceCalls()[0].LockID, ShouldEqual, testLockID)
}

func storeMockEditionCompleteWithLock(instance *models.Instance, expectFirstGetUnlocked bool) (mockedDataStore *storetest.StorerMock, isLocked *bool) {
	mockedDataStore, isLocked = storeMockWithLock(instance, expectFirstGetUnlocked)
	mockedDataStore.GetEditionFunc = func(_ context.Context, _ string, _ string, _ string) (*models.EditionUpdate, error) {
		So(*isLocked, ShouldBeTrue)
		return nil, errs.ErrEditionNotFound
	}
	mockedDataStore.UpsertEditionFunc = func(_ context.Context, _, _ string, _ *models.EditionUpdate) error {
		So(*isLocked, ShouldBeTrue)
		return nil
	}
	mockedDataStore.GetNextVersionFunc = func(context.Context, string, string) (int, error) {
		So(*isLocked, ShouldBeTrue)
		return 1, nil
	}
	mockedDataStore.UpdateInstanceFunc = func(_ context.Context, _ *models.Instance, _ *models.Instance, _ string) (string, error) {
		So(*isLocked, ShouldBeTrue)
		return testETag, nil
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
					So(isLocked, ShouldBeTrue)
				} else {
					So(isLocked, ShouldBeFalse)
				}
			}
			numGetCall++
			return instance, nil
		},
	}
	return mockedDataStore, &isLocked
}
