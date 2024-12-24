package utils

import (
	"context"
	"net/http"
	goURL "net/url"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-net/v2/links"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	datasetAPIURL, _ = goURL.Parse("http://localhost:22000")
	linksBuilder     = links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
)

func TestRewriteDatasetsWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dataset updates", t, func() {
		Convey("When the dataset links need rewriting", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "https://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "https://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Current.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(items[0].Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Current.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				So(items[0].Next.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Next.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				So(items[0].Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Next.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "http://localhost:22000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "http://localhost:22000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Current.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(items[0].Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Current.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				So(items[0].Next.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Next.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				So(items[0].Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Next.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links are nil", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
					},
					Next: &models.Dataset{
						ID: "123",
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links, ShouldBeNil)
				So(items[0].Next.Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID:    "123",
						Links: &models.DatasetLinks{},
					},
					Next: &models.Dataset{
						ID:    "123",
						Links: &models.DatasetLinks{},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links, ShouldResemble, &models.DatasetLinks{})
				So(items[0].Next.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteDatasetsWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dataset updates", t, func() {
		Convey("When the dataset links are unable to be parsed", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, nil)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}
