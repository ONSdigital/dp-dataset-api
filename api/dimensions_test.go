package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
)

func initAPIWithMockedStore(mockedStore *storetest.StorerMock) *DatasetAPI {
	datasetPermissions := getAuthorisationHandlerMock()
	permissions := getAuthorisationHandlerMock()
	return GetAPIWithCMDMocks(mockedStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("When the request contain valid ids return dimension information", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionsFunc: func(ctx context.Context, versionID string) ([]bson.M, error) {
				return []bson.M{}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionsReturnsErrors(t *testing.T) {
	t.Parallel()

	Convey("When the api cannot connect to datastore to get dimension resource return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})

	Convey("When the request contain an invalid version return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})

	Convey("When the request contains an invalid, non-numeric version, return 400 bad request", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/abcd/dimensions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())
	})

	Convey("When there are no dimensions then return not found error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionsFunc: func(ctx context.Context, versionID string) ([]bson.M, error) {
				return nil, errs.ErrDimensionsNotFound
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionsNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})
}

func TestGetDimensionOptionsReturnsOk(t *testing.T) {
	Convey("Given a store with a dimension with 5 options", t, func() {
		// testing DataStore with 5 dimension options
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState, ID: "v1"}, nil
			},
			GetDimensionOptionsFunc: func(ctx context.Context, version *models.Version, dimension string, offset int, limit int) ([]*models.PublicDimensionOption, int, error) {
				allItems := []*models.PublicDimensionOption{
					{Option: "op1"},
					{Option: "op2"},
					{Option: "op3"},
					{Option: "op4"},
					{Option: "op5"}}
				return allItems, 5, nil
			},
			GetDimensionOptionsFromIDsFunc: func(ctx context.Context, version *models.Version, dimension string, ids []string) ([]*models.PublicDimensionOption, int, error) {
				ret := []*models.PublicDimensionOption{}
				sort.Strings(ids)
				for _, id := range ids {
					switch id {
					case "op1":
						ret = append(ret, &models.PublicDimensionOption{Option: "op1"})
					case "op2":
						ret = append(ret, &models.PublicDimensionOption{Option: "op2"})
					case "op3":
						ret = append(ret, &models.PublicDimensionOption{Option: "op3"})
					case "op4":
						ret = append(ret, &models.PublicDimensionOption{Option: "op4"})
					case "op5":
						ret = append(ret, &models.PublicDimensionOption{Option: "op5"})
					}
				}
				return ret, 5, nil
			},
		}

		// func to perform a call
		callOptions := func(r *http.Request) (interface{}, int, error) {
			w := httptest.NewRecorder()
			api := initAPIWithMockedStore(mockedDataStore)
			return api.getDimensionOptions(w, r, 20, 0)
		}

		callOptionsWithIDs := func(r *http.Request) (interface{}, int, error) {
			w := httptest.NewRecorder()
			api := initAPIWithMockedStore(mockedDataStore)
			return api.getDimensionOptions(w, r, 20, 0)
		}

		setExpectedURLVars := func(r *http.Request) *http.Request {
			return mux.SetURLVars(r,
				map[string]string{
					"dataset_id": "123",
					"edition":    "2017",
					"version":    "1",
					"dimension":  "age",
				})
		}

		// func to validate expected calls
		validateCalls := func(expectedIDs *[]string) {
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
			if expectedIDs == nil {
				So(mockedDataStore.GetDimensionOptionsCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetDimensionOptionsFromIDsCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.GetDimensionOptionsCalls()[0].Dimension, ShouldEqual, "age")
				So(mockedDataStore.GetDimensionOptionsCalls()[0].Version.ID, ShouldEqual, "v1")
			} else {
				So(mockedDataStore.GetDimensionOptionsCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.GetDimensionOptionsFromIDsCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetDimensionOptionsFromIDsCalls()[0].Dimension, ShouldEqual, "age")
				So(mockedDataStore.GetDimensionOptionsFromIDsCalls()[0].Version.ID, ShouldEqual, "v1")
				So(mockedDataStore.GetDimensionOptionsFromIDsCalls()[0].Ids, ShouldResemble, *expectedIDs)
			}
		}

		// expected Links structure for the requested dataset version
		expectedLinks := models.DimensionOptionLinks{
			Version: models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1", ID: "1"},
		}

		Convey("When a valid dimension is provided without any query parameters", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", http.NoBody)
			r = setExpectedURLVars(r)
			list, totalCount, err := callOptions(r)

			Convey("Then the call succeeds with 200 OK code, expected body and calls", func() {
				expectedList := []*models.PublicDimensionOption{
					{Option: "op1", Links: expectedLinks},
					{Option: "op2", Links: expectedLinks},
					{Option: "op3", Links: expectedLinks},
					{Option: "op4", Links: expectedLinks},
					{Option: "op5", Links: expectedLinks},
				}
				So(list, ShouldResemble, expectedList)
				So(totalCount, ShouldEqual, 5)
				So(err, ShouldBeNil)
				validateCalls(nil)
			})
		})

		Convey("When a valid dimension and list of existing IDs is provided in more than one parameter, in comma-separated format", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=op1,op3&id=op5", http.NoBody)
			r = setExpectedURLVars(r)
			list, totalCount, err := callOptionsWithIDs(r)

			Convey("Then the call succeeds with 200 OK code, expected body and calls", func() {
				expectedList := []*models.PublicDimensionOption{
					{Option: "op1", Links: expectedLinks},
					{Option: "op3", Links: expectedLinks},
					{Option: "op5", Links: expectedLinks},
				}
				So(list, ShouldResemble, expectedList)
				So(totalCount, ShouldEqual, 5)
				So(err, ShouldBeNil)
				validateCalls(&[]string{"op1", "op3", "op5"})
			})
		})

		Convey("When a valid offset, limit and dimension and list of existing IDs are provided", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=op1,op3&offset=0&limit=1", http.NoBody)
			r = setExpectedURLVars(r)
			list, totalCount, err := callOptionsWithIDs(r)

			Convey("Then the call succeeds with 200 OK code, the list of IDs take precedence (offset and limit are ignored), and the expected body and calls are performed", func() {
				expectedList := []*models.PublicDimensionOption{
					{Option: "op1", Links: expectedLinks},
					{Option: "op3", Links: expectedLinks},
				}

				So(list, ShouldResemble, expectedList)
				So(totalCount, ShouldEqual, 5)
				So(err, ShouldBeNil)
				validateCalls(&[]string{"op1", "op3"})
			})
		})
	})
}

func TestGetDimensionOptionsReturnsErrors(t *testing.T) {
	t.Parallel()

	MaxIDs = func() int { return 5 }

	Convey("Given a set of mocked dependencies", t, func() {
		Convey("Then providing more IDs than the maximum allowed results in 400 BadRequest response", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=id1,id2,id3&id=id4,id5,id6", http.NoBody)
			w := httptest.NewRecorder()

			api := initAPIWithMockedStore(&storetest.StorerMock{})
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrTooManyQueryParameters.Error())
		})
	})

	Convey("When the version doesn't exist in a request for dimension options, then return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an internal error causes failure to retrieve dimension options, then return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFunc: func(ctx context.Context, version *models.Version, dimensions string, offset, limit int) ([]*models.PublicDimensionOption, int, error) {
				return nil, 0, errs.ErrInternalServer
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)
	})

	Convey("When an internal error causes failure to retrieve dimension options from IDs, then return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=id1", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFromIDsFunc: func(ctx context.Context, version *models.Version, dimension string, ids []string) ([]*models.PublicDimensionOption, int, error) {
				return nil, 0, errs.ErrInternalServer
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsFromIDsCalls()), ShouldEqual, 1)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(ctx context.Context, datasetID, edition string, version int, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := initAPIWithMockedStore(mockedDataStore)
		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}
