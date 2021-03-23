package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/globalsign/mgo/bson"
	. "github.com/smartystreets/goconvey/convey"
)

// func to unmarshal and validate body bytes
var validateBody = func(bytes []byte, expected models.DatasetDimensionResults) {
	var response models.DatasetDimensionResults
	err := json.Unmarshal(bytes, &response)
	So(err, ShouldBeNil)
	So(response, ShouldResemble, expected)
}

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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
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
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})

	Convey("When the request contain an invalid version return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionsNotFound.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})
}

func TestGetDimensionOptionsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("Given a store with a dimension with 5 options", t, func() {

		// testing DataStore with 5 dimension options
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState, ID: "v1"}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimensions string, offset, limit int) (*models.DimensionOptionResults, error) {
				if offset > 4 {
					return &models.DimensionOptionResults{
						Items:      []models.PublicDimensionOption{},
						Count:      0,
						TotalCount: 5,
						Limit:      limit,
						Offset:     offset,
					}, nil
				}
				effectiveLimit := limit
				if limit == 0 {
					effectiveLimit = 5
				}
				allItems := []models.PublicDimensionOption{
					{Option: "op1"},
					{Option: "op2"},
					{Option: "op3"},
					{Option: "op4"},
					{Option: "op5"}}
				items := allItems[offset:min(5, offset+effectiveLimit)]
				return &models.DimensionOptionResults{
					Items:      items,
					Count:      len(items),
					TotalCount: 5,
					Limit:      limit,
					Offset:     offset,
				}, nil
			},
			GetDimensionOptionsFromIDsFunc: func(version *models.Version, dimension string, ids []string) (*models.DimensionOptionResults, error) {
				ret := &models.DimensionOptionResults{TotalCount: 5}
				sort.Strings(ids)
				for _, id := range ids {
					switch id {
					case "op1":
						ret.Items = append(ret.Items, models.PublicDimensionOption{Option: "op1"})
					case "op2":
						ret.Items = append(ret.Items, models.PublicDimensionOption{Option: "op2"})
					case "op3":
						ret.Items = append(ret.Items, models.PublicDimensionOption{Option: "op3"})
					case "op4":
						ret.Items = append(ret.Items, models.PublicDimensionOption{Option: "op4"})
					case "op5":
						ret.Items = append(ret.Items, models.PublicDimensionOption{Option: "op5"})
					}
				}
				ret.Count = len(ret.Items)
				return ret, nil
			},
		}

		// permissions mocks
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		// func to perform a call
		call := func(r *http.Request) *httptest.ResponseRecorder {
			w := httptest.NewRecorder()
			api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)
			return w
		}

		// func to validate expected calls
		validateCalls := func(expectedIDs *[]string) {
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
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

		// func to unmarshal and validate body bytes
		validateBody := func(bytes []byte, expected models.DimensionOptionResults) {
			var response models.DimensionOptionResults
			err := json.Unmarshal(bytes, &response)
			So(err, ShouldBeNil)
			So(response, ShouldResemble, expected)
		}

		Convey("When a valid dimension is provided without any query parameters", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
			w := call(r)

			Convey("Then the call succeeds with 200 OK code, expected body and calls", func() {
				expectedResponse := models.DimensionOptionResults{
					Items: []models.PublicDimensionOption{
						{Option: "op1", Links: expectedLinks},
						{Option: "op2", Links: expectedLinks},
						{Option: "op3", Links: expectedLinks},
						{Option: "op4", Links: expectedLinks},
						{Option: "op5", Links: expectedLinks},
					},
					Count:      5,
					Offset:     0,
					Limit:      0,
					TotalCount: 5,
				}
				So(w.Code, ShouldEqual, http.StatusOK)
				validateBody(w.Body.Bytes(), expectedResponse)
				validateCalls(nil)
			})
		})

		Convey("When a valid dimension, limit and offset query parameters are provided, then return dimension information according to the offset and limit", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?offset=2&limit=2", nil)
			w := call(r)

			Convey("Then the call succeeds with 200 OK code, expected body and calls", func() {
				expectedResponse := models.DimensionOptionResults{
					Items: []models.PublicDimensionOption{
						{Option: "op3", Links: expectedLinks},
						{Option: "op4", Links: expectedLinks},
					},
					Count:      2,
					Offset:     2,
					Limit:      2,
					TotalCount: 5,
				}
				So(w.Code, ShouldEqual, http.StatusOK)
				validateBody(w.Body.Bytes(), expectedResponse)
				validateCalls(nil)
			})
		})

		Convey("When a valid dimension, limit above maximum and offset query parameters are provided, then return dimension information according to the offset and limit", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?offset=2&limit=7", nil)
			w := call(r)

			Convey("Then the call succeeds with 200 OK code, expected body and calls", func() {
				expectedResponse := models.DimensionOptionResults{
					Items: []models.PublicDimensionOption{
						{Option: "op3", Links: expectedLinks},
						{Option: "op4", Links: expectedLinks},
						{Option: "op5", Links: expectedLinks},
					},
					Count:      3,
					Offset:     2,
					Limit:      7,
					TotalCount: 5,
				}
				So(w.Code, ShouldEqual, http.StatusOK)
				validateBody(w.Body.Bytes(), expectedResponse)
				validateCalls(nil)
			})
		})

		Convey("When a valid dimension and list of existing IDs is provided in more than one parameter, in comma-separated format", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=op1,op3&id=op5", nil)
			w := call(r)

			Convey("Then the call succeeds with 200 OK code, expected body and calls", func() {
				expectedResponse := models.DimensionOptionResults{
					Items: []models.PublicDimensionOption{
						{Option: "op1", Links: expectedLinks},
						{Option: "op3", Links: expectedLinks},
						{Option: "op5", Links: expectedLinks},
					},
					Count:      3,
					Offset:     0,
					Limit:      0,
					TotalCount: 5,
				}
				So(w.Code, ShouldEqual, http.StatusOK)
				validateBody(w.Body.Bytes(), expectedResponse)
				validateCalls(&[]string{"op1", "op3", "op5"})
			})
		})

		Convey("When a valid offset, limit and dimension and list of existing IDs are provided", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=op1,op3&offset=0&limit=1", nil)
			w := call(r)

			Convey("Then the call succeeds with 200 OK code, the list of IDs take precedence (offset and limit are ignored), and the expected body and calls are performed", func() {
				expectedResponse := models.DimensionOptionResults{
					Items: []models.PublicDimensionOption{
						{Option: "op1", Links: expectedLinks},
						{Option: "op3", Links: expectedLinks},
					},
					Count:      2,
					Offset:     0,
					Limit:      0,
					TotalCount: 5,
				}
				So(w.Code, ShouldEqual, http.StatusOK)
				validateBody(w.Body.Bytes(), expectedResponse)
				validateCalls(&[]string{"op1", "op3"})
			})
		})
	})

}

func TestGetDimensionOptionsReturnsErrors(t *testing.T) {
	t.Parallel()

	MaxIDs = func() int { return 5 }

	Convey("Given a set of mocked dependencies", t, func() {

		// permissions mocks
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		// func to perform a call
		call := func(r *http.Request) *httptest.ResponseRecorder {
			w := httptest.NewRecorder()
			api := GetAPIWithMocks(&storetest.StorerMock{}, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
			api.Router.ServeHTTP(w, r)
			return w
		}

		Convey("When a valid dimension and negative limit and offset query parameters are provided, then return dimension information with limit and offset equal to zero", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?offset=-2&limit=-7", nil)
			w := call(r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidQueryParameter.Error())
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)

		})

		Convey("Then providing wrong value for offset query parameter results in 400 BadRequest response", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?offset=a", nil)
			w := call(r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidQueryParameter.Error())
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
		})

		Convey("Then providing wrong value for limit query parameter results in 400 BadRequest response", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?limit=a", nil)
			w := call(r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidQueryParameter.Error())
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
		})

		Convey("Then providing more IDs than the maximum allowed results in 400 BadRequest response", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=id1,id2,id3&id=id4,id5,id6", nil)
			w := call(r)

			So(w.Code, ShouldEqual, http.StatusBadRequest)
			So(w.Body.String(), ShouldContainSubstring, errs.ErrTooManyQueryParameters.Error())
			So(datasetPermissions.Required.Calls, ShouldEqual, 1)
			So(permissions.Required.Calls, ShouldEqual, 0)
		})
	})

	Convey("When the version doesn't exist in a request for dimension options, then return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an internal error causes failure to retrieve dimension options, then return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimensions string, offset, limit int) (*models.DimensionOptionResults, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)
	})

	Convey("When an internal error causes failure to retrieve dimension options from IDs, then return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options?id=id1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFromIDsFunc: func(version *models.Version, dimension string, ids []string) (*models.DimensionOptionResults, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsFromIDsCalls()), ShouldEqual, 1)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
