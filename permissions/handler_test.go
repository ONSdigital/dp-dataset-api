package permissions

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ONSdigital/go-ns/common"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	serviceAuthToken = "666"
	userAuthToken    = "661"
	collectionID     = "662"
	datasetID        = "663"
)

type HandlerCalls struct {
	W http.ResponseWriter
	R *http.Request
}

var (
	getRequestVarsMoq = func(r *http.Request) map[string]string {
		return map[string]string{
			"dataset_id": datasetID,
		}
	}

	callerPermissions = &CallerPermissions{
		Permissions: []Permission{READ},
	}
)

func TestRequire_authorizedCaller(t *testing.T) {
	/*	wrappedHandlerArgs := make([]HandlerCalls, 0)

		wrappedHandlerFunc := func(w http.ResponseWriter, r *http.Request) {
			wrappedHandlerArgs = append(wrappedHandlerArgs, HandlerCalls{W: w, R: r})
		}*/

	wrappedHandlerCalls := make([]HandlerCalls, 0)
	wrappedHandlerFunc := getProtectedHandlerFuncMoq(&wrappedHandlerCalls)

	policyMoq := &PolicyMock{
		IsSatisfiedFunc: func(ctx context.Context, callerPerms *CallerPermissions, r *http.Request) bool {
			return true
		},
	}

	clientMoq := &ClientMock{
		GetFunc: func(serviceToken string, userToken string, collectionID string, datasetID string) (permissions *CallerPermissions, e error) {
			return callerPermissions, nil
		},
	}

	Convey("given an authorised caller", t, func() {
		permissionsClient = clientMoq
		GetRequestVars = getRequestVarsMoq

		checkPermissions := Require(policyMoq, wrappedHandlerFunc)
		req := getRequestMoq(t)

		resp := httptest.NewRecorder()

		Convey("when a request is received", func() {
			checkPermissions(resp, req)

			So(wrappedHandlerCalls, ShouldHaveLength, 1)
		})

		Convey("then permissionsClient retrieves the callers permissions", func() {
			So(clientMoq.GetCalls(), ShouldHaveLength, 1)
			So(clientMoq.GetCalls()[0].ServiceToken, ShouldEqual, serviceAuthToken)
			So(clientMoq.GetCalls()[0].UserToken, ShouldEqual, userAuthToken)
			So(clientMoq.GetCalls()[0].CollectionID, ShouldEqual, collectionID)
			So(clientMoq.GetCalls()[0].DatasetID, ShouldEqual, datasetID)
		})

		Convey("and the permissions policy confirms the caller satisfies requirements", func() {
			So(policyMoq.IsSatisfiedCalls(), ShouldHaveLength, 1)
			So(policyMoq.IsSatisfiedCalls()[0].Ctx, ShouldNotBeNil)
			So(policyMoq.IsSatisfiedCalls()[0].CallerPerms, ShouldResemble, callerPermissions)
			So(policyMoq.IsSatisfiedCalls()[0].R, ShouldResemble, req)
		})

		Convey("and the protected handler func is invoked with the expected params", func() {
			So(wrappedHandlerCalls, ShouldHaveLength, 1)
			So(wrappedHandlerCalls[0].R, ShouldResemble, req)
			So(wrappedHandlerCalls[0].W, ShouldResemble, resp)
		})
	})
}

func TestRequire_getPermissionsError(t *testing.T) {

	Convey("given the permissionsClient.Get returns error", t, func() {
		clientMoq := &ClientMock{
			GetFunc: func(serviceToken string, userToken string, collectionID string, datasetID string) (permissions *CallerPermissions, e error) {
				return nil, errors.New("pop")
			},
		}

		permissionsClient = clientMoq

		GetRequestVars = getRequestVarsMoq

		policyMoq := &PolicyMock{
			IsSatisfiedFunc: func(ctx context.Context, callerPerms *CallerPermissions, r *http.Request) bool {
				return true
			},
		}

		wrappedHandlerCalls := make([]HandlerCalls, 0)
		wrappedHandler := getProtectedHandlerFuncMoq(&wrappedHandlerCalls)

		checkPermissionsFunc := Require(policyMoq, wrappedHandler)

		Convey("when a request is received", func() {
			req := getRequestMoq(t)
			resp := httptest.NewRecorder()

			checkPermissionsFunc(resp, req)

			Convey("then the permissions client is called 1 time with the expected params", func() {
				So(clientMoq.GetCalls(), ShouldHaveLength, 1)
				So(clientMoq.GetCalls()[0].ServiceToken, ShouldEqual, serviceAuthToken)
				So(clientMoq.GetCalls()[0].UserToken, ShouldEqual, userAuthToken)
				So(clientMoq.GetCalls()[0].CollectionID, ShouldEqual, collectionID)
				So(clientMoq.GetCalls()[0].DatasetID, ShouldEqual, datasetID)
			})

			Convey("and a 500 status is written to the response", func() {
				So(resp.Code, ShouldEqual, 500)
			})

			Convey("and policy.IsSatisfied is never called", func() {
				So(policyMoq.IsSatisfiedCalls(), ShouldHaveLength, 0)
			})

			Convey("and the wrapped handlerFunc is never called", func() {
				So(wrappedHandlerCalls, ShouldHaveLength, 0)
			})

		})

	})

}

func getRequestMoq(t *testing.T) *http.Request {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("error making new request")
	}

	req.Header.Set(common.AuthHeaderKey, serviceAuthToken)
	req.Header.Set(common.FlorenceHeaderKey, userAuthToken)
	req.Header.Set(collectionIDHeader, collectionID)

	return req
}

func getProtectedHandlerFuncMoq(calls *[]HandlerCalls) func(w http.ResponseWriter, r *http.Request) {
	wrappedHandlerFunc := func(w http.ResponseWriter, r *http.Request) {
		*calls = append(*calls, HandlerCalls{W: w, R: r})
	}

	return wrappedHandlerFunc
}
