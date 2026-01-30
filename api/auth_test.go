package api

import (
	"errors"
	"net/http"
	"testing"

	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testEntityData = &permissionsAPISDK.EntityData{
		UserID: "user-1",
		Groups: []string{"group1", "group2"},
	}
)

func TestGetAuthEntityData(t *testing.T) {
	Convey("Given a DatasetAPI instance with a mocked auth middleware", t, func() {
		mockAuthMiddleware := &authMock.MiddlewareMock{
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				if token == "valid-token" {
					return testEntityData, nil
				}
				return nil, errors.New("parse error")
			},
		}

		api := &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
		}

		Convey("When getAuthEntityData is called with a valid access token", func() {
			entityData, err := api.getAuthEntityData("valid-token")

			Convey("Then it should return the expected EntityData and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldResemble, testEntityData)
			})
		})

		Convey("When getAuthEntityData is called with an invalid access token", func() {
			entityData, err := api.getAuthEntityData("invalid-token")

			Convey("Then it should return an error indicating a parse failure", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "failed to parse access token: parse error")
				So(entityData, ShouldBeNil)
			})
		})
	})
}

func TestGetAccessTokenFromRequest(t *testing.T) {
	testCases := []struct {
		name                string
		authorizationHeader string
		expectedToken       string
	}{
		{
			name:                "Valid Bearer token",
			authorizationHeader: "Bearer valid-token",
			expectedToken:       "valid-token",
		},
		{
			name:                "No Bearer prefix",
			authorizationHeader: "valid-token",
			expectedToken:       "valid-token",
		},
		{
			name:                "Empty Authorization header",
			authorizationHeader: "",
			expectedToken:       "",
		},
	}

	for _, tc := range testCases {
		Convey("Given an HTTP request with "+tc.name, t, func() {
			req, err := http.NewRequest("GET", "http://example.com", http.NoBody)
			So(err, ShouldBeNil)

			req.Header.Set(dprequest.AuthHeaderKey, tc.authorizationHeader)

			Convey("When getAccessTokenFromRequest is called", func() {
				token := getAccessTokenFromRequest(req)

				Convey("Then it should return the expected access token", func() {
					So(token, ShouldEqual, tc.expectedToken)
				})
			})
		})
	}
}
