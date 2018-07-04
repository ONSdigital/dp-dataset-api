package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/audit_mock"
	"github.com/ONSdigital/go-ns/common"
	"github.com/gedge/mgo/bson"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("When the request contain valid ids return dimension information", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionsFunc: func(datasetID, versionID string) ([]bson.M, error) {
				return []bson.M{}, nil
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)

		auditParams := common.Params{
			"dataset_id": "123",
			"edition":    "2017",
			"version":    "1",
		}
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetDimensionsReturnsErrors(t *testing.T) {
	auditParams := common.Params{
		"dataset_id": "123",
		"edition":    "2017",
		"version":    "1",
	}

	t.Parallel()
	Convey("When the api cannot connect to datastore to get dimension resource return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the request contain an invalid version return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When there are no dimensions then return not found error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionsFunc: func(datasetID, versionID string) ([]bson.M, error) {
				return nil, errs.ErrDimensionsNotFound
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionsNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)

		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetDimensionsAuditingErrors(t *testing.T) {
	t.Parallel()
	auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1"}

	Convey("given audit action attempted returns an error", t, func() {
		auditor := audit_mock.NewErroring(getDimensionsAction, audit.Attempted)

		Convey("when get dimensions is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					audit_mock.Expected{
						Action: getDimensionsAction,
						Result: audit.Attempted,
						Params: auditParams,
					},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := audit_mock.NewErroring(getDimensionsAction, audit.Successful)

		Convey("when get dimensions is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return &models.Version{State: models.AssociatedState}, nil
				},
				GetDimensionsFunc: func(datasetID, versionID string) ([]bson.M, error) {
					return []bson.M{}, nil
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := audit_mock.NewErroring(getDimensionsAction, audit.Unsuccessful)

		Convey("when datastore.getVersion returns an error", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return nil, errs.ErrVersionNotFound
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})

		Convey("when the version in not in a valid state", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return &models.Version{State: "BROKEN"}, nil
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})

		Convey("when datastore.getDataset returns an error", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return &models.Version{State: models.AssociatedState}, nil
				},
				GetDimensionsFunc: func(datasetID string, versionID string) ([]bson.M, error) {
					return nil, errs.ErrDimensionsNotFound
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionsAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})
	})
}

func TestGetDimensionOptionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("When a valid dimension is provided then a list of options can be returned successfully", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimensions string) (*models.DimensionOptionResults, error) {
				return &models.DimensionOptionResults{}, nil
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)

		auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetDimensionOptionsReturnsErrors(t *testing.T) {
	t.Parallel()
	Convey("When the version doesn't exist in a request for dimension options, then return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 0)

		auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When an internal error causes failure to retrieve dimension options, then return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimensions string) (*models.DimensionOptionResults, error) {
				return nil, errs.ErrInternalServer
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)

		auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 0)

		auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
			audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetDimensionOptionsAuditingErrors(t *testing.T) {
	t.Parallel()

	Convey("given audit action attempted returns an error", t, func() {
		auditor := audit_mock.NewErroring(getDimensionOptionsAction, audit.Attempted)

		Convey("when get dimensions options is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 0)

				auditParams := common.Params{"dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
				auditor.AssertRecordCalls(
					audit_mock.Expected{
						Action: getDimensionOptionsAction,
						Result: audit.Attempted,
						Params: auditParams,
					},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := audit_mock.NewErroring(getDimensionOptionsAction, audit.Successful)

		Convey("when get dimension options is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return &models.Version{State: models.AssociatedState}, nil
				},
				GetDimensionOptionsFunc: func(version *models.Version, dimensions string) (*models.DimensionOptionResults, error) {
					return &models.DimensionOptionResults{}, nil
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)

				auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := audit_mock.NewErroring(getDimensionOptionsAction, audit.Unsuccessful)

		Convey("when datastore.getVersion returns an error", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return nil, errs.ErrVersionNotFound
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

				auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})

		Convey("when the version in not in a valid state", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return &models.Version{State: "BROKEN"}, nil
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)

				auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})

		Convey("when datastore.getDataset returns an error", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
					return &models.Version{State: models.AssociatedState}, nil
				},
				GetDimensionOptionsFunc: func(version *models.Version, dimensions string) (*models.DimensionOptionResults, error) {
					return nil, errs.ErrDimensionNotFound
				},
			}

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)

				auditParams := common.Params{"authorised": "false", "dataset_id": "123", "edition": "2017", "version": "1", "dimension": "age"}
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Attempted, Params: auditParams},
					audit_mock.Expected{Action: getDimensionOptionsAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})
	})
}
