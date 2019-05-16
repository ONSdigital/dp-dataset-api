package permissions

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ONSdigital/go-ns/common"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	serviceAuthToken = "666"
	userAuthToken    = "667"
	collectionID     = "668"
	datasetID        = "669"
)

type handlerCalls struct {
	R *http.Request
	W http.ResponseWriter
}

func TestRequire_CallerAuthorized(t *testing.T) {
	Convey("given an authorized caller", t, func() {
		policyMoq := &PermissionsMock{
			CheckFunc: func(serviceToken string, userToken string, collectionID string, datasetID string) (b bool, e error) {
				return true, nil
			},
		}

		handlerCalls := make([]handlerCalls, 0)
		handler := getHandlerMoq(&handlerCalls)

		GetRequestVars = getRequestVarsMoq()

		checkPermissions := Require(policyMoq, handler)

		req, _ := http.NewRequest("GET", "/something", nil)
		req.Header.Set(common.AuthHeaderKey, serviceAuthToken)
		req.Header.Set(common.FlorenceHeaderKey, userAuthToken)
		req.Header.Set(collectionIDHeader, collectionID)

		w := httptest.NewRecorder()

		Convey("when their request is received", func() {
			checkPermissions(w, req)

			Convey("then the permissions check confirms the caller is authorized", func() {
				So(policyMoq.CheckCalls(), ShouldHaveLength, 1)
				So(policyMoq.CheckCalls()[0].ServiceToken, ShouldEqual, serviceAuthToken)
				So(policyMoq.CheckCalls()[0].UserToken, ShouldEqual, userAuthToken)
				So(policyMoq.CheckCalls()[0].CollectionID, ShouldEqual, collectionID)
				So(policyMoq.CheckCalls()[0].DatasetID, ShouldEqual, datasetID)
			})

			Convey("and the request is allowed to continue", func() {
				So(handlerCalls, ShouldHaveLength, 1)
				So(handlerCalls[0].R, ShouldResemble, req)
				So(handlerCalls[0].W, ShouldResemble, w)
			})
		})
	})
}

func TestRequire_CallerNotAuthorized(t *testing.T) {
	Convey("given an unauthorized caller", t, func() {
		policyMoq := &PermissionsMock{
			CheckFunc: func(serviceToken string, userToken string, collectionID string, datasetID string) (b bool, e error) {
				return false, nil
			},
		}

		handlerCalls := make([]handlerCalls, 0)
		handler := getHandlerMoq(&handlerCalls)

		GetRequestVars = getRequestVarsMoq()

		checkPermissions := Require(policyMoq, handler)

		req, _ := http.NewRequest("GET", "/something", nil)
		req.Header.Set(common.AuthHeaderKey, serviceAuthToken)
		req.Header.Set(common.FlorenceHeaderKey, userAuthToken)
		req.Header.Set(collectionIDHeader, collectionID)

		w := httptest.NewRecorder()

		Convey("when their request is received", func() {
			checkPermissions(w, req)

			Convey("then the permissions check confirms the caller is not authorized to perform the requested action", func() {
				So(policyMoq.CheckCalls(), ShouldHaveLength, 1)
				So(policyMoq.CheckCalls()[0].ServiceToken, ShouldEqual, serviceAuthToken)
				So(policyMoq.CheckCalls()[0].UserToken, ShouldEqual, userAuthToken)
				So(policyMoq.CheckCalls()[0].CollectionID, ShouldEqual, collectionID)
				So(policyMoq.CheckCalls()[0].DatasetID, ShouldEqual, datasetID)
			})

			Convey("and a 401 response is returned", func() {
				So(w.Code, ShouldEqual, 401)
			})

			Convey("and the request does not continue", func() {
				So(handlerCalls, ShouldBeEmpty)
			})
		})
	})
}

func TestRequire_CheckPermissionsError(t *testing.T) {
	Convey("given permissions check returns an error", t, func() {
		policyMoq := &PermissionsMock{
			CheckFunc: func(serviceToken string, userToken string, collectionID string, datasetID string) (b bool, e error) {
				return false, errors.New("wubba lubba dub dub")
			},
		}

		handlerCalls := make([]handlerCalls, 0)
		handler := getHandlerMoq(&handlerCalls)

		GetRequestVars = getRequestVarsMoq()

		checkPermissions := Require(policyMoq, handler)

		req, _ := http.NewRequest("GET", "/something", nil)
		req.Header.Set(common.AuthHeaderKey, serviceAuthToken)
		req.Header.Set(common.FlorenceHeaderKey, userAuthToken)
		req.Header.Set(collectionIDHeader, collectionID)

		w := httptest.NewRecorder()

		Convey("when a request is received", func() {
			checkPermissions(w, req)

			Convey("then the permissions check is called with the expected parameters", func() {
				So(policyMoq.CheckCalls(), ShouldHaveLength, 1)
				So(policyMoq.CheckCalls()[0].ServiceToken, ShouldEqual, serviceAuthToken)
				So(policyMoq.CheckCalls()[0].UserToken, ShouldEqual, userAuthToken)
				So(policyMoq.CheckCalls()[0].CollectionID, ShouldEqual, collectionID)
				So(policyMoq.CheckCalls()[0].DatasetID, ShouldEqual, datasetID)
			})

			Convey("and a 500 response is returned", func() {
				So(w.Code, ShouldEqual, 500)
			})

			Convey("and the request does not continue", func() {
				So(handlerCalls, ShouldBeEmpty)
			})
		})
	})
}

func getHandlerMoq(calls *[]handlerCalls) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		*calls = append(*calls, handlerCalls{R: r, W: w})
	}
}

func getRequestVarsMoq() func(r *http.Request) map[string]string {
	return func(r *http.Request) map[string]string {
		return map[string]string{"dataset_id": datasetID}
	}
}
