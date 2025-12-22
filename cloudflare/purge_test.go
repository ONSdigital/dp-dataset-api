package cloudflare_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/cloudflare"
	"github.com/ONSdigital/dp-dataset-api/cloudflare/mocks"
	"github.com/cloudflare/cloudflare-go/v6/cache"
	"github.com/cloudflare/cloudflare-go/v6/option"
	. "github.com/smartystreets/goconvey/convey"
)

func TestClient_PurgeByPrefixes(t *testing.T) {
	Convey("Given a cloudflare client with a mock CacheService", t, func() {
		ctx := context.Background()

		mockCacheService := mocks.CacheServiceMock{
			PurgeFunc: func(ctx context.Context, params cache.CachePurgeParams, opts ...option.RequestOption) (*cache.CachePurgeResponse, error) {
				return nil, nil
			},
		}

		client := cloudflare.Client{
			ZoneID:       "test-zone-id",
			CacheService: &mockCacheService,
		}

		Convey("When PurgeByPrefixes is called with less than the maximum number of prefixes", func() {
			prefixes := []string{
				"https://example.com/prefix1",
				"https://example.com/prefix2",
			}

			err := client.PurgeByPrefixes(ctx, prefixes)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the Purge method is called once", func() {
				So(len(mockCacheService.PurgeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When PurgeByPrefixes is called with more than the maximum number of prefixes", func() {
			var prefixes []string
			for i := 0; i < 31; i++ {
				prefixes = append(prefixes, "https://example.com/prefix"+strconv.Itoa(i))
			}

			err := client.PurgeByPrefixes(ctx, prefixes)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the Purge method is called multiple times", func() {
				So(len(mockCacheService.PurgeCalls()), ShouldEqual, 2)
			})
		})

		Convey("When PurgeByPrefixes is called and returns an error", func() {
			expectedErr := errors.New("purge failed")

			mockCacheService.PurgeFunc = func(ctx context.Context, params cache.CachePurgeParams, opts ...option.RequestOption) (*cache.CachePurgeResponse, error) {
				return nil, expectedErr
			}

			prefixes := []string{
				"https://example.com/prefix1",
			}

			err := client.PurgeByPrefixes(ctx, prefixes)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldEqual, expectedErr)
			})
		})
	})
}
