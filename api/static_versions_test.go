package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetDatasetEditions_WithQueryParam_Success(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions with a valid query parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?state=associated", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition:      "January",
						EditionTitle: "January Edition Title",
						Version:      1,
						ReleaseDate:  "2025-01-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
					},
					{
						Edition:      "February",
						EditionTitle: "February Edition Title",
						Version:      2,
						ReleaseDate:  "2025-02-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset2"}},
					},
				}, 2, nil
			},
			GetUnpublishedDatasetStaticFunc: func(ctx context.Context, id string) (*models.Dataset, error) {
				if id == "Dataset1" {
					return &models.Dataset{
						ID:    "Dataset1",
						Title: "Test Dataset 1",
					}, nil
				} else if id == "Dataset2" {
					return &models.Dataset{
						ID:    "Dataset2",
						Title: "Test Dataset 2",
					}, nil
				}
				return nil, errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called", func() {
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)
			So(err, ShouldBeNil)

			Convey("Then it should return a 200 status code with the correct count", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(totalCount, ShouldEqual, 2)
				So(results, ShouldHaveLength, 2)
			})

			Convey("And the results should contain the expected fields", func() {
				expectedResponse := []*models.DatasetEdition{
					{
						DatasetID:    "Dataset1",
						Title:        "Test Dataset 1",
						Edition:      "January",
						EditionTitle: "January Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset1/editions/January/versions/1",
							ID:   "1",
						},
						ReleaseDate: "2025-01-01",
					},
					{
						DatasetID:    "Dataset2",
						Title:        "Test Dataset 2",
						Edition:      "February",
						EditionTitle: "February Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset2/editions/February/versions/2",
							ID:   "2",
						},
						ReleaseDate: "2025-02-01",
					},
				}
				So(results, ShouldResemble, expectedResponse)
			})
		})
	})
}

func TestGetDatasetEditions_WithoutQueryParam_Success(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions with a valid query parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition:      "January",
						EditionTitle: "January Edition Title",
						Version:      1,
						ReleaseDate:  "2025-01-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
					},
					{
						Edition:      "February",
						EditionTitle: "February Edition Title",
						Version:      2,
						ReleaseDate:  "2025-02-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset2"}},
					},
				}, 2, nil
			},
			GetUnpublishedDatasetStaticFunc: func(ctx context.Context, id string) (*models.Dataset, error) {
				if id == "Dataset1" {
					return &models.Dataset{
						ID:    "Dataset1",
						Title: "Test Dataset 1",
					}, nil
				} else if id == "Dataset2" {
					return &models.Dataset{
						ID:    "Dataset2",
						Title: "Test Dataset 2",
					}, nil
				}
				return nil, errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called", func() {
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)
			So(err, ShouldBeNil)

			Convey("Then it should return a 200 status code with the correct count", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(totalCount, ShouldEqual, 2)
				So(results, ShouldHaveLength, 2)
			})

			Convey("And the results should contain the expected fields", func() {
				expectedResponse := []*models.DatasetEdition{
					{
						DatasetID:    "Dataset1",
						Title:        "Test Dataset 1",
						Edition:      "January",
						EditionTitle: "January Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset1/editions/January/versions/1",
							ID:   "1",
						},
						ReleaseDate: "2025-01-01",
					},
					{
						DatasetID:    "Dataset2",
						Title:        "Test Dataset 2",
						Edition:      "February",
						EditionTitle: "February Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset2/editions/February/versions/2",
							ID:   "2",
						},
						ReleaseDate: "2025-02-01",
					},
				}
				So(results, ShouldResemble, expectedResponse)
			})
		})
	})
}

func TestGetDatasetEditions_InvalidQueryParam_Failure(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions with an invalid query parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?state=invalid", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called", func() {
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 400 status code with an error message", func() {
				So(err, ShouldEqual, errs.ErrInvalidQueryParameter)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})
	})

	Convey("Given a request to GET /dataset-editions with the query parameter set as published", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?state=published", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called", func() {
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 400 status code with an error message", func() {
				So(err, ShouldEqual, errs.ErrInvalidQueryParameter)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})
	})
}

func TestGetDatasetEditions_GetStaticVersionsByState_Failure(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state string, offset, limit int) ([]*models.Version, int, error) {
				if state == "associated" {
					return nil, 0, errs.ErrVersionsNotFound
				}
				return nil, 0, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called and no versions are found", func() {
			r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?state=associated", http.NoBody)
			w := httptest.NewRecorder()
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 404 status code with an VersionsNotFound error", func() {
				So(err, ShouldEqual, errs.ErrVersionsNotFound)
				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})

		Convey("When getDatasetEditions is called and the datastore fails", func() {
			r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions", http.NoBody)
			w := httptest.NewRecorder()
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 500 status code with an InternalServer error", func() {
				So(err, ShouldEqual, errs.ErrInternalServer)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})
	})
}

func TestGetDatasetEditions_GetUnpublishedDatasetStatic_Failure(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition: "January",
						Links:   &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
					},
				}, 1, nil
			},
			GetUnpublishedDatasetStaticFunc: func(ctx context.Context, id string) (*models.Dataset, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called and the dataset is not found", func() {
			r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions", http.NoBody)
			w := httptest.NewRecorder()
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 404 status code with a DatasetNotFound error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})
	})

	Convey("Given a request to GET /dataset-editions", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition: "January",
						Links:   &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
					},
				}, 1, nil
			},
			GetUnpublishedDatasetStaticFunc: func(ctx context.Context, id string) (*models.Dataset, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called and the datastore fails", func() {
			r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions", http.NoBody)
			w := httptest.NewRecorder()
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 500 status code with an InternalServer error", func() {
				So(err, ShouldEqual, errs.ErrInternalServer)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})
	})
}

func TestAddDatasetVersionCondensed(t *testing.T) {
	t.Parallel()
	Convey("When dataset and edition exist and version is added successfully", t, func() {
		b := `{
			"next_release": "2025-02-15",
			"edition_title": "Edition Title 2025",
			"alerts": [
				{}
			],
			"release_date": "2025-01-15",
			"topics": [
				"Economy",
				"Prices"
			],
			"temporal": [
				{
				"start_date": "2025-01-01",
				"end_date": "2025-01-31",
				"frequency": "Monthly"
				}
			],
			"distributions": [
				{}
			],
			"usage_notes": [
				{
				"title": "Data usage guide",
				"note": "This dataset is subject to revision and should be used in conjunction with the accompanying documentation."
				}
			]
		}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.PublishedState,
				}, nil
			},
			AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
				return &models.Version{Edition: "time-series"}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(successResponse.Status, ShouldEqual, http.StatusCreated)
		So(errorResponse, ShouldBeNil)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
	})

	Convey("When dataset does not exist", t, func() {
		b := `{
				"title": "test-dataset",
				"description": "test dataset",
				"type": "static",
				"next_release": "2025-02-15",
				"edition_title": "Edition Title 2025",
				"alerts": [
					{}
				],
				"latest_changes": [
					{
					"description": "Updated classification of housing components in CPIH.",
					"name": "Changes in classification",
					"type": "Summary of changes"
					}
				],
				"links": {
					"dataset": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd",
					"id": "cpih01"
					},
					"dimensions": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd/dimensions"
					},
					"edition": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series",
					"id": "time-series"
					},
					"job": {
					"href": "http://localhost:10700/jobs/383df410-845e-4efd-9ba1-ab469361eae5",
					"id": "383df410-845e-4efd-9ba1-ab469361eae5"
					},
					"version": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series/versions/1",
					"id": "1"
					},
					"spatial": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd"
					}
				},
				"release_date": "2025-01-15",
				"state": "associated",
				"topics": [
					"Economy",
					"Prices"
				],
				"temporal": [
					{
					"start_date": "2025-01-01",
					"end_date": "2025-01-31",
					"frequency": "Monthly"
					}
				],
				"distributions": [
					{}
				],
				"usage_notes": [
					{
					"title": "Data usage guide",
					"note": "This dataset is subject to revision and should be used in conjunction with the accompanying documentation."
					}
				]
			}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123//editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(errorResponse.Status, ShouldEqual, http.StatusNotFound)
		So(successResponse, ShouldBeNil)
	})
	Convey("When edition does not exist", t, func() {
		b := `{
				"title": "test-dataset",
				"description": "test dataset",
				"type": "static",
				"next_release": "2025-02-15",
				"edition_title": "Edition Title 2025",
				"alerts": [
					{}
				],
				"latest_changes": [
					{
					"description": "Updated classification of housing components in CPIH.",
					"name": "Changes in classification",
					"type": "Summary of changes"
					}
				],
				"links": {
					"dataset": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd",
					"id": "cpih01"
					},
					"dimensions": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd/dimensions"
					},
					"edition": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series",
					"id": "time-series"
					},
					"job": {
					"href": "http://localhost:10700/jobs/383df410-845e-4efd-9ba1-ab469361eae5",
					"id": "383df410-845e-4efd-9ba1-ab469361eae5"
					},
					"version": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd/editions/time-series/versions/1",
					"id": "1"
					},
					"spatial": {
					"href": "http://localhost:10400/datasets/bara-test-ds-abcd"
					}
				},
				"release_date": "2025-01-15",
				"state": "associated",
				"topics": [
					"Economy",
					"Prices"
				],
				"temporal": [
					{
					"start_date": "2025-01-01",
					"end_date": "2025-01-31",
					"frequency": "Monthly"
					}
				],
				"distributions": [
					{}
				],
				"usage_notes": [
					{
					"title": "Data usage guide",
					"note": "This dataset is subject to revision and should be used in conjunction with the accompanying documentation."
					}
				]
			}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errors.New("edition does not exist")
			},
			AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
				return &models.Version{Version: 1, Edition: "time-series"}, nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.PublishedState,
				}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(successResponse.Status, ShouldEqual, http.StatusCreated)
		So(errorResponse, ShouldBeNil)
		So(mockedDataStore.AddVersionStaticCalls(), ShouldHaveLength, 1)

		var response models.Version
		err := json.Unmarshal(successResponse.Body, &response)
		So(err, ShouldBeNil)
		So(response.Version, ShouldEqual, 1)
		So(response.Edition, ShouldEqual, "time-series")
	})

	Convey("When request body is not valid", t, func() {
		b := `{"title":"test-dataset","description":"test dataset","type":"static","next_release":"2025-02-15"}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.AssociatedState,
				}, nil
			},
			AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
				return nil, nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
		So(successResponse, ShouldBeNil)
	})

	Convey("When edition exists, version should increment", t, func() {
		b := `{
				"next_release": "2025-02-15",
				"edition_title": "Edition Title 2025",
				"alerts": [
					{}
				],
				"release_date": "2025-01-15",
				"topics": [
					"Economy",
					"Prices"
				],
				"temporal": [
					{
					"start_date": "2025-01-01",
					"end_date": "2025-01-31",
					"frequency": "Monthly"
					}
				],
				"distributions": [
					{}
				],
				"usage_notes": [
					{
					"title": "Data usage guide",
					"note": "This dataset is subject to revision and should be used in conjunction with the accompanying documentation."
					}
				]
			}`

		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.PublishedState,
				}, nil
			},
			AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
				return &models.Version{Version: 2}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getAuthorisationHandlerMock(), getAuthorisationHandlerMock())
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(successResponse.Status, ShouldEqual, http.StatusCreated)
		So(errorResponse, ShouldBeNil)

		var response models.Version
		err := json.Unmarshal(successResponse.Body, &response)
		So(err, ShouldBeNil)
		So(response.Version, ShouldEqual, 2)
	})
}

func TestAddDatasetVersionCondensedReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When an unpublished version already exists, it returns a 400 error", t, func() {
		b := `{
				"release_date": "2025-01-15",
				"edition_title": "test-edition",
				"distributions": [{}]
			}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return &models.Version{
					Version: 1,
					State:   models.AssociatedState,
				}, nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		castErr := errorResponse.Errors[0]
		So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
		So(castErr.Code, ShouldEqual, models.ErrVersionAlreadyExists)
		So(successResponse, ShouldBeNil)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetLatestVersionStaticCalls(), ShouldHaveLength, 1)
	})

	Convey("When the latest version of the dataset is published, it creates a new version", t, func() {
		b := `{
				"release_date": "2025-01-15",
				"edition_title": "Edition Title 2025",
				"distributions": [{}]
			}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return &models.Version{
					Version: 1,
					State:   models.PublishedState,
				}, nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: "associated"}}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
				return &models.Version{Version: 2}, nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(successResponse.Status, ShouldEqual, http.StatusCreated)
		So(errorResponse, ShouldBeNil)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetLatestVersionStaticCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.AddVersionStaticCalls(), ShouldHaveLength, 1)
	})

	Convey("When an error occurs checking the latest version", t, func() {
		b := `{
				"release_date": "2025-01-15",
				"edition_title": "Edition Title 2025",
				"distributions": [{}]
			}`
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/time-series/versions", bytes.NewBufferString(b))
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(testContext, w, r)

		So(errorResponse.Status, ShouldEqual, http.StatusInternalServerError)
		So(successResponse, ShouldBeNil)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetLatestVersionStaticCalls(), ShouldHaveLength, 1)
	})
}
