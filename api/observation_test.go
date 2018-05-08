package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-filter/observation"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	expectedObservationDoc = `{` +
		`"dimensions":{` +
		`"aggregate":{"options":[{"id":"cpi1dim1S40403","href":"http://localhost:8081/code-lists/cpih1dim1aggid/codes/cpi1dim1S40403"}]},` +
		`"geography":{"options":[{"id":"K02000001","href":"http://localhost:8081/code-lists/uk-only/codes/K02000001"}]},` +
		`"time":{"options":[{"id":"16-Aug","href":"http://localhost:8081/code-lists/time/codes/16-Aug"}]}},` +
		`"links":{` +
		`"dataset_metadata":{"href":"http://localhost:8080/datasets/cpih012/editions/2017/versions/1/metadata"},` +
		`"self":{"href":"http://localhost:8080/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001"},` +
		`"version":{"id":"1","href":"http://localhost:8080/datasets/cpih012/editions/2017/versions/1"}},` +
		`"observation":"146.3",` +
		`"observation_level_metadata":{"confidence_interval":"2","data_marking":"p"},` +
		`"usage_notes":[{"title":"data_marking","note":"this marks the obsevation with a special character"}]` +
		"}\n"
)

func TestGetObservationsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get a single observation for a version of a dataset returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:8080/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug\x26aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()

		dimensions := []models.CodeList{
			models.CodeList{
				Name: "aggregate",
				HRef: "http://localhost:8081/code-lists/cpih1dim1aggid",
			},
			models.CodeList{
				Name: "geography",
				HRef: "http://localhost:8081/code-lists/uk-only",
			},
			models.CodeList{
				Name: "time",
				HRef: "http://localhost:8081/code-lists/time",
			},
		}
		usagesNotes := &[]models.UsageNote{models.UsageNote{Title: "data_marking", Note: "this marks the obsevation with a special character"}}

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					Dimensions: dimensions,
					Headers:    []string{"v4_2", "data_marking", "confidence_interval", "aggregate_code", "aggregate", "geography_code", "geography", "time", "time"},
					Links: &models.VersionLinks{
						Version: &models.LinkObject{
							HRef: "http://localhost:8080/datasets/cpih012/editions/2017/versions/1",
							ID:   "1",
						},
					},
					State:      models.PublishedState,
					UsageNotes: usagesNotes,
				}, nil
			},
		}

		count := 0
		mockRowReader := &mocks.CSVRowReaderMock{
			ReadFunc: func() (string, error) {
				if count == 0 {
					count++
					return "v4_2,data_marking,confidence_interval,time,time,geography_code,geography,aggregate_code,aggregate", nil
				} else if count == 1 {
					count++
					return "146.3,p,2,Month,Aug-16,K02000001,,cpi1dim1G10100,01.1 Food", nil
				}
				return "", io.EOF
			},
			CloseFunc: func() error {
				return nil
			},
		}

		mockedObservationStore := &mocks.ObservationStoreMock{
			GetCSVRowsFunc: func(*observation.Filter, *int) (observation.CSVRowReader, error) {
				return mockRowReader, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(w.Body.String(), ShouldEqual, expectedObservationDoc)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedObservationStore.GetCSVRowsCalls()), ShouldEqual, 1)
		So(len(mockRowReader.ReadCalls()), ShouldEqual, 3)
	})
}

func TestGetObservationsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to mongo datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errInternal
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset exists but is unpublished return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an unpublished version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "Incorrect resource state\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When a version document has not got a headers field return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{State: models.PublishedState}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When the first header in array does not describe the header row correctly return internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{Headers: []string{"v4"}, State: models.PublishedState}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "index out of range\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an invalid query parameter is set in request return 400 bad request with an error message containing a list of incorrect query parameters", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{Headers: []string{"v4_0", "time_code", "time", "aggregate_code", "aggregate"}, State: models.PublishedState}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "Incorrect selection of query parameters: [geography], these dimensions do not exist for this version of the dataset\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When there is a missing query parameter that is expected to be set in request return 400 bad request with an error message containing a list of missing query parameters", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{Headers: []string{"v4_0", "time_code", "time", "aggregate_code", "aggregate", "geography_code", "geography", "age_code", "age"}, State: models.PublishedState}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "Missing query parameters for the following dimensions: [age]\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When requested query does not find a unique observation return observation not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{Headers: []string{"v4_0", "time_code", "time", "aggregate_code", "aggregate", "geography_code", "geography"}, State: models.PublishedState}, nil
			},
		}

		mockedObservationStore := &mocks.ObservationStoreMock{
			GetCSVRowsFunc: func(*observation.Filter, *int) (observation.CSVRowReader, error) {
				return nil, errs.ErrObservationsNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Observation not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedObservationStore.GetCSVRowsCalls()), ShouldEqual, 1)
	})

	Convey("When requested query finds more than 1 observation return more than one observation found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/cpih012/editions/2017/versions/1/observations?time=16-Aug&aggregate=cpi1dim1S40403&geography=K02000001", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{Headers: []string{"v4_0", "time_code", "time", "aggregate_code", "aggregate", "geography_code", "geography"}, State: models.PublishedState}, nil
			},
		}

		count := 0
		mockRowReader := &mocks.CSVRowReaderMock{
			ReadFunc: func() (string, error) {
				if count == 0 {
					count++
					return "v4_0,time,time,geography_code,geography,aggregate_code,aggregate", nil
				} else if count <= 2 {
					count++
					return "146.3,Month,Aug-16,K02000001,,cpi1dim1G10100,01.1 Food", nil
				}
				return "", io.EOF
			},
			CloseFunc: func() error {
				return nil
			},
		}

		mockedObservationStore := &mocks.ObservationStoreMock{
			GetCSVRowsFunc: func(*observation.Filter, *int) (observation.CSVRowReader, error) {
				return mockRowReader, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockedObservationStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "More than one observation found, add more query parameters\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedObservationStore.GetCSVRowsCalls()), ShouldEqual, 1)
		So(len(mockRowReader.ReadCalls()), ShouldEqual, 3)
	})
}

func TestgetListOfValidDimensionNames(t *testing.T) {
	t.Parallel()
	Convey("Given the version headers are valid", t, func() {
		Convey("When the version has no meta data headers", func() {
			version := &models.Version{
				Headers: []string{
					"v4_0",
					"time_codelist",
					"time",
					"aggregate_codelist",
					"Aggregate",
					"geography_codelist",
					"geography",
				},
			}

			Convey("Then getListOfValidDimensionNames func returns the correct number of headers", func() {
				dimensionOptions, dimensionOffset, err := getListOfValidDimensionNames(version.Headers)

				So(err, ShouldBeNil)
				So(dimensionOffset, ShouldEqual, 0)
				So(len(dimensionOptions), ShouldEqual, 3)
				So(dimensionOptions[0], ShouldEqual, "time")
				So(dimensionOptions[1], ShouldEqual, "Aggregate")
				So(dimensionOptions[2], ShouldEqual, "geography")
			})
		})

		Convey("When the version has meta data headers", func() {
			version := &models.Version{
				Headers: []string{
					"V4_2",
					"data_marking",
					"confidence_interval",
					"time_codelist",
					"time",
				},
			}

			Convey("Then getListOfValidDimensionNames func returns the correct number of headers", func() {
				dimensionOptions, dimensionOffset, err := getListOfValidDimensionNames(version.Headers)

				So(err, ShouldBeNil)
				So(dimensionOffset, ShouldEqual, 2)
				So(len(dimensionOptions), ShouldEqual, 1)
				So(dimensionOptions[0], ShouldEqual, "time")
			})
		})
	})

	Convey("Given the first value in the header does not have an underscore `_` in value", t, func() {
		Convey("When the getListOfValidDimensionNames func is called", func() {
			version := &models.Version{
				Headers: []string{
					"v4",
					"time_codelist",
					"time",
					"aggregate_codelist",
					"aggregate",
					"geography_codelist",
					"geography",
				},
			}
			Convey("Then function returns error, `index out of range`", func() {
				dimensionOptions, dimensionOffset, err := getListOfValidDimensionNames(version.Headers)

				So(err, ShouldNotBeNil)
				So(dimensionOffset, ShouldEqual, 0)
				So(err.Error(), ShouldResemble, "index out of range")
				So(dimensionOptions, ShouldBeNil)
			})
		})
	})

	Convey("Given the first value in the header does not follow the format `v4_1`", t, func() {
		Convey("When the getListOfValidDimensionNames func is called", func() {
			version := &models.Version{
				Headers: []string{
					"v4_one",
					"time_codelist",
					"time",
					"aggregate_codelist",
					"aggregate",
					"geography_codelist",
					"geography",
				},
			}
			Convey("Then function returns error, `index out of range`", func() {
				dimensionOptions, dimensionOffset, err := getListOfValidDimensionNames(version.Headers)

				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "strconv.Atoi: parsing \"one\": invalid syntax")
				So(dimensionOffset, ShouldEqual, 0)
				So(dimensionOptions, ShouldBeNil)
			})
		})
	})
}

func TestExtractQueryParameters(t *testing.T) {
	t.Parallel()
	Convey("Given a list of valid dimension headers for version", t, func() {
		headers := []string{
			"time",
			"aggregate",
			"geography",
		}

		Convey("When a request is made containing query parameters for each dimension/header", func() {
			r, err := http.NewRequest("GET",
				"http://localhost:22000/datasets/123/editions/2017/versions/1/observations?time=JAN08&aggregate=Overall Index&geography=wales",
				nil,
			)
			So(err, ShouldBeNil)

			Convey("Then extractQueryParameters func returns a list of query parameters and there corresponding value", func() {
				queryParameters, err := extractQueryParameters(r.URL.Query(), headers)
				So(err, ShouldBeNil)
				So(len(queryParameters), ShouldEqual, 3)
				So(queryParameters["time"], ShouldEqual, "JAN08")
				So(queryParameters["aggregate"], ShouldEqual, "Overall Index")
				So(queryParameters["geography"], ShouldEqual, "wales")
			})
		})

		Convey("When a request is made containing query parameters for 2/3 dimensions/headers", func() {
			r, err := http.NewRequest("GET",
				"http://localhost:22000/datasets/123/editions/2017/versions/1/observations?time=JAN08&geography=wales",
				nil,
			)
			So(err, ShouldBeNil)

			Convey("Then extractQueryParameters func returns an error", func() {
				queryParameters, err := extractQueryParameters(r.URL.Query(), headers)
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errorMissingQueryParameters([]string{"aggregate"}))
				So(queryParameters, ShouldBeNil)
			})
		})

		Convey("When a request is made containing all query parameters for each dimensions/headers but also an invalid one", func() {
			r, err := http.NewRequest("GET",
				"http://localhost:22000/datasets/123/editions/2017/versions/1/observations?time=JAN08&aggregate=Food&geography=wales&age=52",
				nil,
			)
			So(err, ShouldBeNil)

			Convey("Then extractQueryParameters func returns an error", func() {
				queryParameters, err := extractQueryParameters(r.URL.Query(), headers)
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errorIncorrectQueryParameters([]string{"age"}))
				So(queryParameters, ShouldBeNil)
			})
		})
	})
}
