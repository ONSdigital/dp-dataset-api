package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	healthcheck "github.com/ONSdigital/dp-api-clients-go/v2/health"
	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
	. "github.com/smartystreets/goconvey/convey"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
)

var (
	testEntityData = &permissionsAPISDK.EntityData{
		UserID: "user-1",
		Groups: []string{"group1", "group2"},
	}

	testServiceEntityData = &permissionsAPISDK.EntityData{
		UserID: "service-1",
	}
)

func newMockHTTPClient(retCode int, retBody interface{}) *dphttp.ClienterMock {
	return &dphttp.ClienterMock{
		SetPathsWithNoRetriesFunc: func(paths []string) {},
		GetPathsWithNoRetriesFunc: func() []string {
			return []string{"/healthcheck"}
		},
		DoFunc: func(ctx context.Context, req *http.Request) (*http.Response, error) {
			body, _ := json.Marshal(retBody)
			return &http.Response{
				StatusCode: retCode,
				Body:       io.NopCloser(bytes.NewReader(body)),
			}, nil
		},
	}
}

var testIdentityResponse = &dprequest.IdentityResponse{
	Identifier: "myIdentity",
}

var testServiceIdentityResponse = &dprequest.IdentityResponse{
	Identifier: "service-1",
}

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

		httpClient := newMockHTTPClient(200, testIdentityResponse)
		idClient := clientsidentity.NewWithHealthClient(healthcheck.NewClientWithClienter("", "http://localhost:8082", httpClient))

		api := &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       idClient,
		}

		rValid := http.Request{
			Header: http.Header{},
		}
		rValid.Header.Set("Authorization", "Bearer valid-token")

		Convey("When getAuthEntityData is called with a valid access token", func() {
			entityData, err := api.getAuthEntityData(&rValid)

			Convey("Then it should return the expected EntityData and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldResemble, testEntityData)
			})
		})

		httpClientUnauthorised := newMockHTTPClient(403, testIdentityResponse)
		idClientUnauthorised := clientsidentity.NewWithHealthClient(healthcheck.NewClientWithClienter("", "http://localhost:8082", httpClientUnauthorised))

		api = &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       idClientUnauthorised,
		}

		rNotValid := http.Request{
			Header: http.Header{},
		}
		rNotValid.Header.Set("Authorization", "Bearer invalid-token")

		Convey("When getAuthEntityData is called with an invalid access token", func() {
			entityData, err := api.getAuthEntityData(&rNotValid)

			Convey("Then it should return an error indicating a parse failure", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "failed to parse access token: unexpected status code returned from AuthAPI: unable to determine the user or service making the request")
				So(entityData, ShouldBeNil)
			})
		})
	})

	Convey("Given a DatasetAPI instance with a mocked auth middleware that fails jwt auth", t, func() {
		mockAuthMiddleware := &authMock.MiddlewareMock{
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, errors.New("parse error")
			},
		}

		httpClient := newMockHTTPClient(200, testServiceIdentityResponse)
		idClient := clientsidentity.NewWithHealthClient(healthcheck.NewClientWithClienter("", "http://localhost:8082", httpClient))

		api := &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       idClient,
		}

		rValid := http.Request{
			Header: http.Header{},
		}
		rValid.Header.Set("Authorization", "Bearer valid-service-token")

		Convey("When getAuthEntityData is called with a valid service token", func() {
			entityData, err := api.getAuthEntityData(&rValid)

			Convey("Then it should return the expected EntityData and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldResemble, testServiceEntityData)
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
