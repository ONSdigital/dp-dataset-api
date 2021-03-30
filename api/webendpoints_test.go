package api

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/globalsign/mgo/bson"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

// The follow unit tests check that when ENABLE_PRIVATE_ENDPOINTS is set to false, only
// published datasets are returned, even if the secret token is set.

var testContext = context.Background()

func TestWebSubnetDatasetsEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets", nil)
		So(err, ShouldBeNil)

		current := &models.Dataset{ID: "1234", Title: "current"}
		next := &models.Dataset{ID: "4321", Title: "next"}

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(ctx context.Context, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error) {
				return []*models.DatasetUpdate{
					{
						Current: current,
						Next:    next,
					},
				}, 0, nil
			},
		}
		Convey("Calling the datasets endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			a, _ := ioutil.ReadAll(w.Body)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
			var results models.DatasetResults
			json.Unmarshal(a, &results)
			// Only a single dataset should be returned in a web subnet
			So(len(results.Items), ShouldEqual, 1)
			So(results.Items[0].Title, ShouldEqual, current.Title)
		})
	})
}

func TestWebSubnetDatasetEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234", nil)
		So(err, ShouldBeNil)

		current := &models.Dataset{ID: "1234", Title: "current"}
		next := &models.Dataset{ID: "1234", Title: "next"}

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(ID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Current: current,
					Next:    next,
				}, nil
			},
		}
		Convey("Calling the dataset endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			a, _ := ioutil.ReadAll(w.Body)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
			var result models.Dataset
			json.Unmarshal(a, &result)
			So(result.Title, ShouldEqual, current.Title)
		})
	})
}

func TestWebSubnetEditionsEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234/editions", nil)
		So(err, ShouldBeNil)

		edition := models.EditionUpdate{ID: "1234", Current: &models.Edition{State: models.PublishedState}}
		var editionSearchState, datasetSearchState string

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID, state string) error {
				datasetSearchState = state
				return nil
			},
			GetEditionsFunc: func(ctx context.Context, ID, state string, offset, limit int, authorised bool) ([]*models.EditionUpdate, int, error) {
				editionSearchState = state
				return []*models.EditionUpdate{&edition}, 0, nil
			},
		}
		Convey("Calling the editions endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetSearchState, ShouldEqual, models.PublishedState)
			So(editionSearchState, ShouldEqual, models.PublishedState)
		})
	})
}

func TestWebSubnetEditionEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234/editions/1234", nil)
		So(err, ShouldBeNil)

		edition := &models.EditionUpdate{ID: "1234", Current: &models.Edition{State: models.PublishedState}}
		var editionSearchState, datasetSearchState string

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID, state string) error {
				datasetSearchState = state
				return nil
			},
			GetEditionFunc: func(ID, editionID, state string) (*models.EditionUpdate, error) {
				editionSearchState = state
				return edition, nil
			},
		}
		Convey("Calling the edition endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetSearchState, ShouldEqual, models.PublishedState)
			So(editionSearchState, ShouldEqual, models.PublishedState)
		})
	})
}

func TestWebSubnetVersionsEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234/editions/1234/versions", nil)
		So(err, ShouldBeNil)

		var versionSearchState, editionSearchState, datasetSearchState string
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID, state string) error {
				datasetSearchState = state
				return nil
			},
			CheckEditionExistsFunc: func(ID, editionID, state string) error {
				editionSearchState = state
				return nil
			},
			GetVersionsFunc: func(ctx context.Context, id string, editionID string, state string, offset, limit int) (*models.VersionResults, error) {
				versionSearchState = state
				return &models.VersionResults{
					Items: []models.Version{{ID: "124", State: models.PublishedState}},
				}, nil
			},
		}
		Convey("Calling the versions endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetSearchState, ShouldEqual, models.PublishedState)
			So(editionSearchState, ShouldEqual, models.PublishedState)
			So(versionSearchState, ShouldEqual, models.PublishedState)
		})
	})
}

func TestWebSubnetVersionEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234/editions/1234/versions/1234", nil)
		So(err, ShouldBeNil)

		var versionSearchState, editionSearchState, datasetSearchState string
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID, state string) error {
				datasetSearchState = state
				return nil
			},
			CheckEditionExistsFunc: func(ID, editionID, state string) error {
				editionSearchState = state
				return nil
			},
			GetVersionFunc: func(id string, editionID, version string, state string) (*models.Version, error) {
				versionSearchState = state
				return &models.Version{ID: "124", State: models.PublishedState,
					Links: &models.VersionLinks{
						Version: &models.LinkObject{},
						Self:    &models.LinkObject{}}}, nil
			},
		}
		Convey("Calling the version endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusOK)
			So(datasetSearchState, ShouldEqual, models.PublishedState)
			So(editionSearchState, ShouldEqual, models.PublishedState)
			So(versionSearchState, ShouldEqual, models.PublishedState)
		})
	})
}

func TestWebSubnetDimensionsEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234/editions/1234/versions/1234/dimensions", nil)
		So(err, ShouldBeNil)

		var versionSearchState string
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(id string, editionID, version string, state string) (*models.Version, error) {
				versionSearchState = state
				return &models.Version{ID: "124", State: models.PublishedState,
					Links: &models.VersionLinks{
						Version: &models.LinkObject{},
						Self:    &models.LinkObject{}}}, nil
			},
			GetDimensionsFunc: func(datasetID string, versionID string) ([]bson.M, error) {
				return []bson.M{}, nil
			},
		}
		Convey("Calling dimension endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(versionSearchState, ShouldEqual, models.PublishedState)
		})
	})
}

func TestWebSubnetDimensionOptionsEndpoint(t *testing.T) {
	Convey("When the API is started with private endpoints disabled", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/1234/editions/1234/versions/1234/dimensions/t/options", nil)
		So(err, ShouldBeNil)

		var versionSearchState string
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(id string, editionID, version string, state string) (*models.Version, error) {
				versionSearchState = state
				return &models.Version{ID: "124", State: models.PublishedState,
					Links: &models.VersionLinks{
						Version: &models.LinkObject{},
						Self:    &models.LinkObject{}}}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimension string, offset, limit int) (*models.DimensionOptionResults, error) {
				return &models.DimensionOptionResults{Items: []models.PublicDimensionOption{}}, nil
			},
		}

		Convey("Calling dimension option endpoint should allow only published items", func() {

			api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

			api.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(versionSearchState, ShouldEqual, models.PublishedState)
		})
	})
}

func TestPublishedSubnetEndpointsAreDisabled(t *testing.T) {

	type testEndpoint struct {
		Method string
		URL    string
	}

	publishSubnetEndpoints := map[testEndpoint]int{
		// Dataset Endpoints
		{Method: "POST", URL: "http://localhost:22000/datasets/1234"}:                            http.StatusMethodNotAllowed,
		{Method: "PUT", URL: "http://localhost:22000/datasets/1234"}:                             http.StatusMethodNotAllowed,
		{Method: "PUT", URL: "http://localhost:22000/datasets/1234/editions/1234/versions/2123"}: http.StatusMethodNotAllowed,

		// Instance endpoints
		{Method: "GET", URL: "http://localhost:22000/instances"}:                            http.StatusNotFound,
		{Method: "POST", URL: "http://localhost:22000/instances"}:                           http.StatusNotFound,
		{Method: "GET", URL: "http://localhost:22000/instances/1234"}:                       http.StatusNotFound,
		{Method: "PUT", URL: "http://localhost:22000/instances/123"}:                        http.StatusNotFound,
		{Method: "PUT", URL: "http://localhost:22000/instances/123/dimensions/test"}:        http.StatusNotFound,
		{Method: "POST", URL: "http://localhost:22000/instances/1/events"}:                  http.StatusNotFound,
		{Method: "PUT", URL: "http://localhost:22000/instances/1/inserted_observations/11"}: http.StatusNotFound,
		{Method: "PUT", URL: "http://localhost:22000/instances/1/import_tasks"}:             http.StatusNotFound,

		// Dimension endpoints
		{Method: "GET", URL: "http://localhost:22000/instances/1/dimensions"}:                       http.StatusNotFound,
		{Method: "POST", URL: "http://localhost:22000/instances/1/dimensions"}:                      http.StatusNotFound,
		{Method: "GET", URL: "http://localhost:22000/instances/1/dimensions/1/options"}:             http.StatusNotFound,
		{Method: "PUT", URL: "http://localhost:22000/instances/1/dimensions/1/options/1/node_id/1"}: http.StatusNotFound,
	}

	Convey("When the API is started with private endpoints disabled", t, func() {

		for endpoint, expectedStatusCode := range publishSubnetEndpoints {
			Convey("The following endpoint "+endpoint.URL+"(Method:"+endpoint.Method+") should return 404", func() {
				r, err := createRequestWithAuth(endpoint.Method, endpoint.URL, nil)
				So(err, ShouldBeNil)

				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{}

				api := GetWebAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, nil, nil)

				api.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, expectedStatusCode)
			})
		}
	})
}

func GetWebAPIWithMocks(ctx context.Context, mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, datasetPermissions AuthHandler, permissions AuthHandler) *DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEndpoints = false

	return Setup(ctx, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, datasetPermissions, permissions)
}
