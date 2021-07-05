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

func Test_UpdateInstanceToEditionConfirmedReturnsOk(t *testing.T) {
	Convey("Given a PUT request to update an instance resource", t, func() {
		Convey("When the requested state change is to 'edition-confirmed'", func() {
			Convey("Then return status ok (200)", func() {
				body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				currentInstanceTest_Data := &models.Instance{
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

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
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
					UpdateInstanceFunc: func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
						return testETag, nil
					},
					AddVersionDetailsToInstanceFunc: func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
						return nil
					},
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				//	So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)
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

				currentInstanceTest_Data := &models.Instance{
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

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
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
					UpdateInstanceFunc: func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
						return testETag, nil
					},
					AddVersionDetailsToInstanceFunc: func(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
						return errors.New("boom")
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
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
						Dataset: &models.LinkObject{
							ID: "4567",
						},
					},
					State: models.EditionConfirmedState,
				}

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					UpdateInstanceFunc: func(ctx context.Context, currentInstance *models.Instance, updatedInstance *models.Instance, eTagSelector string) (string, error) {
						return testETag, nil
					},
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrExpectedResourceStateOfSubmitted.Error())
				So(datasetPermissions.Required.Calls, ShouldEqual, 0)
				So(permissions.Required.Calls, ShouldEqual, 1)

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), ShouldEqual, 0)
			})
		})
	})
}
