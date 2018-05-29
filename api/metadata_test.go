package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/common"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetMetadataReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Successfully return metadata resource for a request without an authentication header", t, func() {

		datasetDoc := createDatasetDoc()
		versionDoc := createVersionDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetID, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		calls := auditor.RecordCalls()
		ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], getMetadataAction, actionSuccessful, ap)

		bytes, err := ioutil.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(bytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldBeNil)
		So(metaData.ReleaseFrequency, ShouldEqual, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
	})

	// Subtle difference between the test above and below, keywords is Not nil
	// in response for test below while it is nil in test above
	Convey("Successfully return metadata resource for a request with an authentication header", t, func() {

		datasetDoc := createDatasetDoc()
		versionDoc := createVersionDoc()

		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetID, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		auditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		calls := auditor.RecordCalls()
		ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], getMetadataAction, actionSuccessful, ap)

		bytes, err := ioutil.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(bytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldResemble, []string{"pensioners"})
		So(metaData.ReleaseFrequency, ShouldResemble, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
	})
}

func TestGetMetadataReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errInternal
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document cannot be found for version return status not found", t, func() {

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document has no current sub document return status not found", t, func() {

		datasetDoc := createDatasetDoc()
		datasetDoc.Current = nil

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition document cannot be found for version return status not found", t, func() {

		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Edition not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the version document cannot be found return status not found", t, func() {

		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Version not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When the version document state is invalid return an internal server error", t, func() {

		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "Incorrect resource state\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetMetadataAuditingErrors(t *testing.T) {
	ap := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}

	Convey("given auditing action attempted returns an error", t, func() {
		auditor := getMockAuditorFunc(func(a string, r string) error {
			if a == getMetadataAction && r == actionAttempted {
				return errors.New("audit error")
			}
			return nil
		})

		Convey("when get metadata is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 1)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("given auditing action unsuccessful returns an error", t, func() {
		auditor := getMockAuditorFunc(func(a string, r string) error {
			if a == getMetadataAction && r == actionUnsuccessful {
				return errors.New("audit error")
			}
			return nil
		})

		Convey("when datastore getDataset returns dataset not found error", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 404 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], getMetadataAction, actionUnsuccessful, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
			})
		})

		Convey("when dataset.current is empty", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{}, nil
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 404 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], getMetadataAction, actionUnsuccessful, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
			})
		})

		Convey("when dataset edition does not exist", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
					return createDatasetDoc(), nil
				},
				CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
					return errs.ErrEditionNotFound
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 404 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], getMetadataAction, actionUnsuccessful, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
			})
		})

		Convey("when dataset version does not exist", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
					return createDatasetDoc(), nil
				},
				CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
					return nil
				},
				GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
					return nil, errs.ErrVersionNotFound
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], getMetadataAction, actionUnsuccessful, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
			})
		})

		Convey("when version not published", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()
			versionDoc := createVersionDoc()
			versionDoc.State = "not published"

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
					return createDatasetDoc(), nil
				},
				CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
					return nil
				},
				GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
					return versionDoc, nil
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], getMetadataAction, actionUnsuccessful, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
			})
		})
	})

	Convey("given auditing action successful returns an error", t, func() {
		auditor := getMockAuditorFunc(func(a string, r string) error {
			if a == getMetadataAction && r == actionSuccessful {
				return errors.New("audit error")
			}
			return nil
		})

		Convey("when get metadata is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
					return createDatasetDoc(), nil
				},
				CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
					return nil
				},
				GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
					return createVersionDoc(), nil
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)

				calls := auditor.RecordCalls()
				So(len(calls), ShouldEqual, 2)
				verifyAuditRecordCalls(calls[0], getMetadataAction, actionAttempted, ap)
				verifyAuditRecordCalls(calls[1], getMetadataAction, actionSuccessful, ap)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
			})
		})

	})

}

// createDatasetDoc returns a datasetUpdate doc containing minimal fields but
// there is a clear difference between the current and next sub documents
func createDatasetDoc() *models.DatasetUpdate {
	generalDataset := &models.Dataset{
		CollectionID:     "4321",
		ReleaseFrequency: "yearly",
		State:            models.PublishedState,
		UnitOfMeasure:    "Pounds Sterling",
	}

	nextDataset := *generalDataset
	nextDataset.CollectionID = "3434"
	nextDataset.Keywords = []string{"pensioners"}
	nextDataset.State = models.AssociatedState

	datasetDoc := &models.DatasetUpdate{
		ID:      "123",
		Current: generalDataset,
		Next:    &nextDataset,
	}

	return datasetDoc
}

func createVersionDoc() *models.Version {
	temporal := models.TemporalFrequency{
		EndDate:   "2017-05-09",
		Frequency: "Monthly",
		StartDate: "2014-05-09",
	}
	versionDoc := &models.Version{
		State:        models.EditionConfirmedState,
		CollectionID: "3434",
		Temporal:     &[]models.TemporalFrequency{temporal},
	}

	return versionDoc
}
