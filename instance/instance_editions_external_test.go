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

			Convey("Then the dp-graph function is called", func() {
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)
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

			Convey("Then the dp-graph function is not called", func() {
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)
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
				mockedDataStore.AddVersionDetailsToInstanceFunc = func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
					So(*isLocked, ShouldBeTrue)
					return errors.New("boom")
				}

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
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)
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
				mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
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
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)
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

func storeMockEditionCompleteWithLock(instance *models.Instance, expectFirstGetUnlocked bool) (*storetest.StorerMock, *bool) {
	mockedDataStore, isLocked := storeMockWithLock(instance, expectFirstGetUnlocked)
	mockedDataStore.GetEditionFunc = func(ctx context.Context, datasetID string, edition string, state string) (*models.EditionUpdate, error) {
		So(*isLocked, ShouldBeTrue)
		return nil, errs.ErrEditionNotFound
	}
	mockedDataStore.UpsertEditionFunc = func(ctx context.Context, datasetID, edition string, editionDoc *models.EditionUpdate) error {
		So(*isLocked, ShouldBeTrue)
		return nil
	}
	mockedDataStore.GetNextVersionFunc = func(context.Context, string, string) (int, error) {
		So(*isLocked, ShouldBeTrue)
		return 1, nil
	}
	mockedDataStore.UpdateInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
		So(*isLocked, ShouldBeTrue)
		return testETag, nil
	}
	mockedDataStore.AddVersionDetailsToInstanceFunc = func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
		So(*isLocked, ShouldBeTrue)
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

func storeMockWithLock(instance *models.Instance, expectFirstGetUnlocked bool) (*storetest.StorerMock, *bool) {
	isLocked := false
	numGetCall := 0
	return &storetest.StorerMock{
		AcquireInstanceLockFunc: func(ctx context.Context, instanceID string) (string, error) {
			isLocked = true
			return testLockID, nil
		},
		UnlockInstanceFunc: func(ctx context.Context, lockID string) {
			isLocked = false
		},
		GetInstanceFunc: func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
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
	}, &isLocked
}
