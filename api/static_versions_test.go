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
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetDatasetEditions_WithQueryParam_Success(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions with a valid query parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?state=associated", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state, published string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition:      "January",
						EditionTitle: "January Edition Title",
						Version:      1,
						ReleaseDate:  "2025-01-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
						State:        models.AssociatedState,
					},
					{
						Edition:      "February",
						EditionTitle: "February Edition Title",
						Version:      2,
						ReleaseDate:  "2025-02-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset2"}},
						State:        models.AssociatedState,
					},
				}, 2, nil
			},
			GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
				if id == "Dataset1" {
					return &models.DatasetUpdate{
						ID: "Dataset1",
						Next: &models.Dataset{
							Title:       "Test Dataset 1",
							Description: "Test dataset 1 description",
						},
					}, nil
				} else if id == "Dataset2" {
					return &models.DatasetUpdate{
						ID: "Dataset2",
						Next: &models.Dataset{
							Title:       "Test Dataset 2",
							Description: "Test dataset 2 description",
						},
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
						Description:  "Test dataset 1 description",
						Edition:      "January",
						EditionTitle: "January Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset1/editions/January/versions/1",
							ID:   "1",
						},
						ReleaseDate: "2025-01-01",
						State:       models.AssociatedState,
					},
					{
						DatasetID:    "Dataset2",
						Title:        "Test Dataset 2",
						Description:  "Test dataset 2 description",
						Edition:      "February",
						EditionTitle: "February Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset2/editions/February/versions/2",
							ID:   "2",
						},
						ReleaseDate: "2025-02-01",
						State:       models.AssociatedState,
					},
				}
				So(results, ShouldResemble, expectedResponse)
			})
		})
	})
}

func TestGetDatasetEditions_WithPublishedParam(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions with a valid published parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?published=true", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state, published string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition:      "January",
						EditionTitle: "January Edition Title",
						Version:      1,
						ReleaseDate:  "2025-01-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
						State:        models.PublishedState,
					},
				}, 1, nil
			},
			GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "Dataset1",
					Next: &models.Dataset{
						Title:       "Test Dataset 1",
						Description: "Test dataset 1 description",
					},
				}, nil
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
				So(totalCount, ShouldEqual, 1)
				So(results, ShouldHaveLength, 1)
			})

			Convey("And the results should contain the expected fields", func() {
				expectedResponse := []*models.DatasetEdition{
					{
						DatasetID:    "Dataset1",
						Title:        "Test Dataset 1",
						Description:  "Test dataset 1 description",
						Edition:      "January",
						EditionTitle: "January Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset1/editions/January/versions/1",
							ID:   "1",
						},
						ReleaseDate: "2025-01-01",
						State:       models.PublishedState,
					},
				}
				So(results, ShouldResemble, expectedResponse)
			})
		})
	})

	Convey("Given a request to GET /dataset-editions with an invalid published parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?published=123", http.NoBody)
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

	Convey("Given a request to GET /dataset-editions with both state and published param", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions?published=true&state=associated", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		Convey("When getDatasetEditions is called", func() {
			results, totalCount, err := api.getDatasetEditions(w, r, 20, 0)

			Convey("Then it should return a 400 status code with an error message", func() {
				So(err, ShouldEqual, errs.ErrInvalidParamCombination)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(totalCount, ShouldEqual, 0)
				So(results, ShouldBeNil)
			})
		})
	})
}

func TestGetDatasetEditions_WithoutQueryParam_Success(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions with no query parameter", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/dataset-editions", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state, published string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition:      "January",
						EditionTitle: "January Edition Title",
						Version:      1,
						ReleaseDate:  "2025-01-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
						State:        models.AssociatedState,
					},
					{
						Edition:      "February",
						EditionTitle: "February Edition Title",
						Version:      2,
						ReleaseDate:  "2025-02-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset2"}},
						State:        models.EditionConfirmedState,
					},
					{
						Edition:      "March",
						EditionTitle: "March Edition Title",
						Version:      1,
						ReleaseDate:  "2025-02-01",
						Links:        &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset3"}},
						State:        models.PublishedState,
					},
				}, 3, nil
			},
			GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
				if id == "Dataset1" {
					return &models.DatasetUpdate{
						ID: "Dataset1",
						Next: &models.Dataset{
							Title:       "Test Dataset 1",
							Description: "Test dataset 1 description",
						},
					}, nil
				}
				if id == "Dataset2" {
					return &models.DatasetUpdate{
						ID: "Dataset2",
						Next: &models.Dataset{
							Title:       "Test Dataset 2",
							Description: "Test dataset 2 description",
						},
					}, nil
				}
				if id == "Dataset3" {
					return &models.DatasetUpdate{
						ID: "Dataset3",
						Next: &models.Dataset{
							Title:       "Test Dataset 3",
							Description: "Test dataset 3 description",
						},
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
				So(totalCount, ShouldEqual, 3)
				So(results, ShouldHaveLength, 3)
			})

			Convey("And the results should contain the expected fields including a published version", func() {
				expectedResponse := []*models.DatasetEdition{
					{
						DatasetID:    "Dataset1",
						Title:        "Test Dataset 1",
						Description:  "Test dataset 1 description",
						Edition:      "January",
						EditionTitle: "January Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset1/editions/January/versions/1",
							ID:   "1",
						},
						ReleaseDate: "2025-01-01",
						State:       models.AssociatedState,
					},
					{
						DatasetID:    "Dataset2",
						Title:        "Test Dataset 2",
						Description:  "Test dataset 2 description",
						Edition:      "February",
						EditionTitle: "February Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset2/editions/February/versions/2",
							ID:   "2",
						},
						ReleaseDate: "2025-02-01",
						State:       models.EditionConfirmedState,
					},
					{
						DatasetID:    "Dataset3",
						Title:        "Test Dataset 3",
						Description:  "Test dataset 3 description",
						Edition:      "March",
						EditionTitle: "March Edition Title",
						LatestVersion: models.LinkObject{
							HRef: "/datasets/Dataset3/editions/March/versions/1",
							ID:   "1",
						},
						ReleaseDate: "2025-02-01",
						State:       models.PublishedState,
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
}

func TestGetDatasetEditions_GetStaticVersionsByState_Failure(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state, published string, offset, limit int) ([]*models.Version, int, error) {
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

func TestGetDatasetEditions_GetDataset_Failure(t *testing.T) {
	t.Parallel()
	Convey("Given a request to GET /dataset-editions", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetStaticVersionsByStateFunc: func(ctx context.Context, state, published string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition: "January",
						Links:   &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
					},
				}, 1, nil
			},
			GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
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
			GetStaticVersionsByStateFunc: func(ctx context.Context, state, published string, offset, limit int) ([]*models.Version, int, error) {
				return []*models.Version{
					{
						Edition: "January",
						Links:   &models.VersionLinks{Dataset: &models.LinkObject{ID: "Dataset1"}},
					},
				}, 1, nil
			},
			GetDatasetFunc: func(ctx context.Context, id string) (*models.DatasetUpdate, error) {
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

func TestAddDatasetVersionCondensed_Success(t *testing.T) {
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
				return &models.Version{Edition: "time-series", Type: models.Static.String()}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

		So(successResponse.Status, ShouldEqual, http.StatusCreated)
		So(errorResponse, ShouldBeNil)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		Convey("Then the created version should have type 'static'", func() {
			var version models.Version
			err := json.Unmarshal(successResponse.Body, &version)
			So(err, ShouldBeNil)
			So(version.Type, ShouldEqual, models.Static.String())
		})
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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

		So(successResponse.Status, ShouldEqual, http.StatusCreated)
		So(errorResponse, ShouldBeNil)

		var response models.Version
		err := json.Unmarshal(successResponse.Body, &response)
		So(err, ShouldBeNil)
		So(response.Version, ShouldEqual, 2)
	})
}

func TestAddDatasetVersionCondensed_Failure(t *testing.T) {
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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

		So(errorResponse.Status, ShouldEqual, http.StatusInternalServerError)
		So(successResponse, ShouldBeNil)
		So(mockedDataStore.CheckDatasetExistsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetLatestVersionStaticCalls(), ShouldHaveLength, 1)
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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

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

		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

		So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
		So(successResponse, ShouldBeNil)
	})

	Convey("When dataset and edition exist and version type is not static", t, func() {
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
			],
			"type": "filterable"
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
		successResponse, errorResponse := api.addDatasetVersionCondensed(w, r)

		So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
		err := errorResponse.Errors[0]
		So(err.Code, ShouldEqual, models.ErrInvalidTypeError)
		So(err.Description, ShouldEqual, models.ErrInvalidType)
		So(successResponse, ShouldBeNil)
	})
}

func TestCreateVersion_Success(t *testing.T) {
	t.Parallel()

	validVersion := &models.Version{
		EditionTitle: "New edition title",
		ReleaseDate:  "2025-01-01",
		Distributions: &[]models.Distribution{
			{
				Title:       "Distribution 1",
				Format:      "csv",
				DownloadURL: "path/to/download/1",
				ByteSize:    100,
				MediaType:   "text/csv",
			},
			{
				Title:       "Distribution 2",
				Format:      "csv",
				DownloadURL: "path/to/download/2",
				ByteSize:    200,
				MediaType:   "text/csv",
			},
		},
		QualityDesignation: models.QualityDesignationOfficial,
		Alerts: &[]models.Alert{
			{
				Description: "First alert",
				Type:        models.AlertTypeAlert,
			},
			{
				Description: "First correction",
				Type:        models.AlertTypeCorrection,
			},
		},
		UsageNotes: &[]models.UsageNote{
			{
				Note:  "Note 1",
				Title: "Usage Note 1",
			},
			{
				Note:  "Note 2",
				Title: "Usage Note 2",
			},
		},
	}

	expectedVersion := &models.Version{
		EditionTitle: "New edition title",
		ReleaseDate:  "2025-01-01",
		Distributions: &[]models.Distribution{
			{
				Title:       "Distribution 1",
				Format:      "csv",
				DownloadURL: "path/to/download/1",
				ByteSize:    100,
				MediaType:   "text/csv",
			},
			{
				Title:       "Distribution 2",
				Format:      "csv",
				DownloadURL: "path/to/download/2",
				ByteSize:    200,
				MediaType:   "text/csv",
			},
		},
		QualityDesignation: models.QualityDesignationOfficial,
		Alerts: &[]models.Alert{
			{
				Description: "First alert",
				Type:        models.AlertTypeAlert,
			},
			{
				Description: "First correction",
				Type:        models.AlertTypeCorrection,
			},
		},
		UsageNotes: &[]models.UsageNote{
			{
				Note:  "Note 1",
				Title: "Usage Note 1",
			},
			{
				Note:  "Note 2",
				Title: "Usage Note 2",
			},
		},
		Edition: "edition1",
		Version: 1,
		Type:    models.Static.String(),
		State:   models.AssociatedState,
		Links: &models.VersionLinks{
			Dataset: &models.LinkObject{
				ID:   "123",
				HRef: "/datasets/123",
			},
			Edition: &models.LinkObject{
				ID:   "edition1",
				HRef: "/datasets/123/editions/edition1",
			},
			Version: &models.LinkObject{
				ID:   "1",
				HRef: "/datasets/123/editions/edition1/versions/1",
			},
			Self: &models.LinkObject{
				ID:   "1",
				HRef: "/datasets/123/editions/edition1/versions/1",
			},
		},
		ETag: "etag",
	}

	mockedDataStore := &storetest.StorerMock{
		CheckDatasetExistsFunc: func(context.Context, string, string) error {
			return nil
		},
		CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
			return nil
		},
		CheckVersionExistsStaticFunc: func(context.Context, string, string, int) (bool, error) {
			return false, nil
		},
		AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
			return expectedVersion, nil
		},
	}

	datasetPermissions := getAuthorisationHandlerMock()
	permissions := getAuthorisationHandlerMock()

	Convey("Given a valid request to createVersion", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		returnedVersionJSON, err := json.Marshal(expectedVersion)
		So(err, ShouldBeNil)

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 201 status code with the created version", func() {
				So(errorResponse, ShouldBeNil)
				So(successResponse.Status, ShouldEqual, http.StatusCreated)
				So(successResponse.Body, ShouldEqual, returnedVersionJSON)
			})

			Convey("And the eTag header should be set", func() {
				So(w.Header().Get("ETag"), ShouldEqual, "etag")
			})
		})
	})
}

func TestCreateVersion_Failure(t *testing.T) {
	t.Parallel()

	validVersion := &models.Version{
		EditionTitle: "New edition title",
		ReleaseDate:  "2025-01-01",
		Distributions: &[]models.Distribution{
			{
				Title:       "Distribution 1",
				Format:      "csv",
				DownloadURL: "path/to/download/1",
				ByteSize:    100,
				MediaType:   "text/csv",
			},
			{
				Title:       "Distribution 2",
				Format:      "csv",
				DownloadURL: "path/to/download/2",
				ByteSize:    200,
				MediaType:   "text/csv",
			},
		},
		QualityDesignation: models.QualityDesignationOfficial,
		Alerts: &[]models.Alert{
			{
				Description: "First alert",
				Type:        models.AlertTypeAlert,
			},
			{
				Description: "First correction",
				Type:        models.AlertTypeCorrection,
			},
		},
		UsageNotes: &[]models.UsageNote{
			{
				Note:  "Note 1",
				Title: "Usage Note 1",
			},
			{
				Note:  "Note 2",
				Title: "Usage Note 2",
			},
		},
	}

	mockedDataStore := &storetest.StorerMock{}
	datasetPermissions := getAuthorisationHandlerMock()
	permissions := getAuthorisationHandlerMock()

	Convey("When the JSON body provided is invalid", t, func() {
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/invalid", http.NoBody)
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 400 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.JSONUnmarshalError)
				So(errorResponse.Errors[0].Description, ShouldEqual, "failed to unmarshal version")
			})
		})
	})

	Convey("When the version number provided is invalid", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/invalid", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "invalid",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 400 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.ErrInvalidQueryParameter)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.ErrInvalidQueryParameterDescription+": version")
			})
		})
	})

	Convey("When all mandatory fields are not provided", t, func() {
		invalidVersion := &models.Version{
			Version: 1,
		}
		invalidVersionJSON, err := json.Marshal(invalidVersion)
		So(err, ShouldBeNil)

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/100", bytes.NewBuffer(invalidVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 400 status code with all the mandatory field errors", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusBadRequest)
				So(errorResponse.Errors, ShouldHaveLength, 3)

				So(errorResponse.Errors[0].Code, ShouldEqual, models.ErrMissingParameters)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.ErrMissingParametersDescription+": release_date")
				So(errorResponse.Errors[1].Code, ShouldEqual, models.ErrMissingParameters)
				So(errorResponse.Errors[1].Description, ShouldEqual, models.ErrMissingParametersDescription+": distributions")
				So(errorResponse.Errors[2].Code, ShouldEqual, models.ErrMissingParameters)
				So(errorResponse.Errors[2].Description, ShouldEqual, models.ErrMissingParametersDescription+": edition_title")
			})
		})
	})

	Convey("When the dataset does not exist", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 404 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusNotFound)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.ErrDatasetNotFound)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.ErrDatasetNotFoundDescription)
			})
		})
	})

	Convey("When checking if the dataset exists returns an error", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrInternalServer
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 500 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusInternalServerError)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.InternalError)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.InternalErrorDescription)
			})
		})
	})

	Convey("When the edition does not exist", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 404 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusNotFound)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.ErrEditionNotFound)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.ErrEditionNotFoundDescription)
			})
		})
	})

	Convey("When checking if the edition exists returns an error", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrInternalServer
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 500 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusInternalServerError)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.InternalError)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.InternalErrorDescription)
			})
		})
	})

	Convey("When the version already exists", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			CheckVersionExistsStaticFunc: func(context.Context, string, string, int) (bool, error) {
				return true, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 409 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusConflict)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.ErrVersionAlreadyExists)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.ErrVersionAlreadyExistsDescription)
			})
		})
	})

	Convey("When checking if the version exists returns an error", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			CheckVersionExistsStaticFunc: func(context.Context, string, string, int) (bool, error) {
				return false, errs.ErrInternalServer
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 500 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusInternalServerError)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.InternalError)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.InternalErrorDescription)
			})
		})
	})

	Convey("When adding the version to the datastore returns an error", t, func() {
		validVersionJSON, err := json.Marshal(validVersion)
		So(err, ShouldBeNil)

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			CheckVersionExistsStaticFunc: func(context.Context, string, string, int) (bool, error) {
				return false, nil
			},
			AddVersionStaticFunc: func(context.Context, *models.Version) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		r := createRequestWithAuth("POST", "http://localhost:22000/datasets/123/editions/edition1/versions/1", bytes.NewBuffer(validVersionJSON))
		vars := map[string]string{
			"dataset_id": "123",
			"edition":    "edition1",
			"version":    "1",
		}
		r = mux.SetURLVars(r, vars)
		w := httptest.NewRecorder()

		Convey("When createVersion is called", func() {
			successResponse, errorResponse := api.createVersion(w, r)

			Convey("Then it should return a 500 status code with an error message", func() {
				So(successResponse, ShouldBeNil)
				So(errorResponse.Status, ShouldEqual, http.StatusInternalServerError)
				So(errorResponse.Errors[0].Code, ShouldEqual, models.InternalError)
				So(errorResponse.Errors[0].Description, ShouldEqual, models.InternalErrorDescription)
			})
		})
	})
}
