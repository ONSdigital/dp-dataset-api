package instance_test

import (
	"context"
	"errors"
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

func Test_UpdateInstanceToEditionConfirmedReturnsOk(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}

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
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				//	So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
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

func Test_UpdateInstanceToEditionConfirmedReturnsError(t *testing.T) {
	auditParams := common.Params{"instance_id": "123"}
	auditParamsWithCallerIdentity := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
	editionAuditParams := common.Params{"instance_id": "123", "dataset_id": "4567", "edition": "2017"}

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
				//	So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				//			So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
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
						Dataset: &models.LinkObject{
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

		Convey(`When the requested state change is to 'edition-confirmed' and attempt to audit request to create edition fails`, func() {
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
				//			So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 0)
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
										ID:   "test",
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
				//		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
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
					GetInstanceFunc: func(id string) (*models.Instance, error) {
						return currentInstanceTest_Data, nil
					},
					GetEditionFunc: func(datasetID string, edition string, state string) (*models.EditionUpdate, error) {
						return &models.EditionUpdate{
							Current: &models.Edition{
								State: models.PublishedState,
								Links: &models.EditionUpdateLinks{
									LatestVersion: &models.LinkObject{
										ID: "1",
									},
									Self: &models.LinkObject{
										HRef: "/edition/test/href",
									},
								},
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
										ID:   "test",
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

				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 3)
				So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
				//			So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
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
