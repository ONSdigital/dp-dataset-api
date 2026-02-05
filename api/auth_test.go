package api

import (
	"context"
	"errors"
	"net/http"
	"testing"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
	identityClientMock "github.com/ONSdigital/dp-api-clients-go/v2/identity/mock"
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

	testIdentityResponse = &dprequest.IdentityResponse{
		Identifier: "identifier",
	}
)

func TestGetAuthEntityData(t *testing.T) {
	Convey("Given a DatasetAPI instance with a mocked dependencies", t, func() {
		mockAuthMiddleware := &authMock.MiddlewareMock{
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				if token == "valid-token" {
					return testEntityData, nil
				}
				return nil, errors.New("parse error")
			},
		}

		mockIdentityClient := &identityClientMock.TokenIdentityMock{
			CheckTokenIdentityFunc: func(ctx context.Context, token string, tokenType clientsidentity.TokenType) (*dprequest.IdentityResponse, error) {
				if token == "service-token" {
					return testIdentityResponse, nil
				}
				return nil, errors.New("identity check error")
			},
		}

		api := &DatasetAPI{
			authMiddleware: mockAuthMiddleware,
			idClient:       mockIdentityClient,
		}

		ctx := context.Background()

		Convey("When getAuthEntityData is called with a valid user access token", func() {
			entityData, err := api.getAuthEntityData(ctx, "valid-token")

			Convey("Then it should return the expected EntityData and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldResemble, testEntityData)
				So(mockAuthMiddleware.ParseCalls(), ShouldHaveLength, 1)
				So(mockIdentityClient.CheckTokenIdentityCalls(), ShouldHaveLength, 0)
			})
		})

		Convey("When getAuthEntityData is called with a valid service access token", func() {
			entityData, err := api.getAuthEntityData(ctx, "service-token")

			Convey("Then it should return EntityData with the UserID set to the identifier from the identity response and no error", func() {
				So(err, ShouldBeNil)
				So(entityData, ShouldNotBeNil)
				So(entityData.UserID, ShouldEqual, testIdentityResponse.Identifier)
				So(mockAuthMiddleware.ParseCalls(), ShouldHaveLength, 1)
				So(mockIdentityClient.CheckTokenIdentityCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("When getAuthEntityData is called with an invalid access token", func() {
			entityData, err := api.getAuthEntityData(ctx, "invalid-token")

			Convey("Then it should return an error indicating a parse failure", func() {
				So(err, ShouldNotBeNil)
				// Parse error and identity check error are wrapped
				So(err.Error(), ShouldContainSubstring, "parse error")
				So(err.Error(), ShouldContainSubstring, "failed to check token identity: identity check error")
				So(entityData, ShouldBeNil)
				So(mockAuthMiddleware.ParseCalls(), ShouldHaveLength, 1)
				So(mockIdentityClient.CheckTokenIdentityCalls(), ShouldHaveLength, 1)
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
