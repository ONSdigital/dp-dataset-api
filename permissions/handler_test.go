package permissions

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ONSdigital/go-ns/common"
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

func TestRequire_CallerHasPermissions(t *testing.T) {
	Convey("given an authorized caller", t, func() {
		policyMoq := &PolicyMock{
			CheckCallerFunc: func(serviceToken string, userToken string, collectionID string, datasetID string) (b bool, e error) {
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

			Convey("then the policy confirms the caller has the required permissions", func() {
				So(policyMoq.CheckCallerCalls(), ShouldHaveLength, 1)
				So(policyMoq.CheckCallerCalls()[0].ServiceToken, ShouldEqual, serviceAuthToken)
				So(policyMoq.CheckCallerCalls()[0].UserToken, ShouldEqual, userAuthToken)
				So(policyMoq.CheckCallerCalls()[0].CollectionID, ShouldEqual, collectionID)
				So(policyMoq.CheckCallerCalls()[0].DatasetID, ShouldEqual, datasetID)
			})

			Convey("and the request is allowed to continue", func() {
				So(handlerCalls, ShouldHaveLength, 1)
				So(handlerCalls[0].R, ShouldResemble, req)
				So(handlerCalls[0].W, ShouldResemble, w)
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
