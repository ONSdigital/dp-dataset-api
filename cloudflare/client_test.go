package cloudflare_test

import (
	"testing"

	"github.com/ONSdigital/dp-dataset-api/cloudflare"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNew(t *testing.T) {
	Convey("Given valid API token and zone ID", t, func() {
		apiToken := "test-token"
		zoneID := "test-zone-id"
		useSDK := false

		Convey("When creating a new Cloudflare client", func() {
			client, err := cloudflare.New(apiToken, zoneID, useSDK)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the client is not nil", func() {
				So(client, ShouldNotBeNil)
			})
		})
	})

	Convey("Given an empty API token", t, func() {
		apiToken := ""
		zoneID := "test-zone-id"
		useSDK := false

		Convey("When creating a new Cloudflare client", func() {
			client, err := cloudflare.New(apiToken, zoneID, useSDK)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "API token is required")
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})

	Convey("Given an empty zone ID", t, func() {
		apiToken := "test-token"
		zoneID := ""
		useSDK := false

		Convey("When creating a new Cloudflare client", func() {
			client, err := cloudflare.New(apiToken, zoneID, useSDK)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "zone ID is required")
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})

	Convey("Given both empty API token and zone ID", t, func() {
		apiToken := ""
		zoneID := ""
		useSDK := false

		Convey("When creating a new Cloudflare client", func() {
			client, err := cloudflare.New(apiToken, zoneID, useSDK)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})
}
