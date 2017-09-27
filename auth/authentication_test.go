package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMiddleWareAuthenticationReturnsForbidden(t *testing.T) {
	t.Parallel()
	Convey("When no access token is provide, unauthorised status code is returned", t, func() {
		auth := &Authenticator{"123", "internal-token"}
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		auth.Check(mockHTTPHandler).ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
	})
}

func TestMiddleWareAuthenticationReturnsUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("When a invalid access token is provide, unauthorised status code is returned", t, func() {
		auth := &Authenticator{"123", "internal-token"}
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", nil)
		r.Header.Set("internal-token", "12")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		auth.Check(mockHTTPHandler).ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
	})
}

func TestMiddleWareAuthentication(t *testing.T) {
	t.Parallel()
	Convey("When a valid access token is provide, OK code is returned", t, func() {
		auth := &Authenticator{"123", "internal-token"}
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", nil)
		r.Header.Set("internal-token", "123")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		auth.Check(mockHTTPHandler).ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
	})
}

func TestMiddleWareAuthenticationWithValue(t *testing.T) {
	t.Parallel()
	Convey("When a valid access token is provide, true is passed to a http handler", t, func() {
		auth := &Authenticator{"123", "internal-token"}
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", nil)
		r.Header.Set("internal-token", "123")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		var isRequestAuthenticated bool
		auth.ManualCheck(func(w http.ResponseWriter, r *http.Request, isAuth bool) {
			isRequestAuthenticated = isAuth
		}).ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(isRequestAuthenticated, ShouldEqual, true)
	})
}

func mockHTTPHandler(w http.ResponseWriter, r *http.Request) {

}
