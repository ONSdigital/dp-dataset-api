package sdk

import (
	"net/http"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_AddHeaders(t *testing.T) {
	testCases := []struct {
		name     string
		headers  Headers
		expected http.Header
	}{
		{
			name: "all headers set",
			headers: Headers{
				CollectionID:         "test-collection-id",
				DownloadServiceToken: "test-download-token",
				AccessToken:          "test-access-token",
				IfMatch:              "test-etag",
			},
			expected: http.Header{
				"Collection-Id":            []string{"test-collection-id"},
				"X-Download-Service-Token": []string{"test-download-token"},
				"Authorization":            []string{"Bearer test-access-token"},
				"If-Match":                 []string{"test-etag"},
			},
		},
		{
			name:     "no headers set",
			headers:  Headers{},
			expected: http.Header{},
		},
		{
			name: "some headers set",
			headers: Headers{
				CollectionID: "test-collection-id",
				AccessToken:  "test-access-token",
			},
			expected: http.Header{
				"Collection-Id": []string{"test-collection-id"},
				"Authorization": []string{"Bearer test-access-token"},
			},
		},
		{
			name: "AccessToken with Bearer prefix",
			headers: Headers{
				AccessToken: "Bearer test-access-token",
			},
			expected: http.Header{
				"Authorization": []string{"Bearer test-access-token"},
			},
		},
	}

	for _, tc := range testCases {
		Convey("When Add is called with "+tc.name, t, func() {
			req, err := http.NewRequest("GET", "http://example.com", nil)
			So(err, ShouldBeNil)

			tc.headers.add(req)

			So(req.Header, ShouldResemble, tc.expected)
		})
	}
}
