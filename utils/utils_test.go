package utils

import (
	"context"
	"net/http"
	goURL "net/url"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-net/v2/links"
	"github.com/smartystreets/goconvey/convey"
)

var (
	codeListAPIURL, _ = goURL.Parse("http://localhost:22400")
	datasetAPIURL, _  = goURL.Parse("http://localhost:22000")
	importAPIURL, _   = goURL.Parse("http://localhost:21800")
	websiteURL, _     = goURL.Parse("http://localhost:20000")

	codeListLinksBuilder = links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
	datasetLinksBuilder  = links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
	importLinksBuilder   = links.FromHeadersOrDefault(&http.Header{}, importAPIURL)
	websiteLinksBuilder  = links.FromHeadersOrDefault(&http.Header{}, websiteURL)
)

func TestRewriteDatasetsWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dataset updates", t, func() {
		convey.Convey("When the dataset links need rewriting", func() {
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

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Current.Links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(items[0].Current.Links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(items[0].Current.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				convey.So(items[0].Current.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(items[0].Current.Links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				convey.So(items[0].Next.Links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(items[0].Next.Links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(items[0].Next.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				convey.So(items[0].Next.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(items[0].Next.Links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		convey.Convey("When the dataset links do not need rewriting", func() {
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

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Current.Links.AccessRights.HRef, convey.ShouldEqual, results[0].Current.Links.AccessRights.HRef)
				convey.So(items[0].Current.Links.Editions.HRef, convey.ShouldEqual, results[0].Current.Links.Editions.HRef)
				convey.So(items[0].Current.Links.LatestVersion.HRef, convey.ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				convey.So(items[0].Current.Links.Self.HRef, convey.ShouldEqual, results[0].Current.Links.Self.HRef)
				convey.So(items[0].Current.Links.Taxonomy.HRef, convey.ShouldEqual, results[0].Current.Links.Taxonomy.HRef)
				convey.So(items[0].Next.Links.AccessRights.HRef, convey.ShouldEqual, results[0].Next.Links.AccessRights.HRef)
				convey.So(items[0].Next.Links.Editions.HRef, convey.ShouldEqual, results[0].Next.Links.Editions.HRef)
				convey.So(items[0].Next.Links.LatestVersion.HRef, convey.ShouldEqual, results[0].Next.Links.LatestVersion.HRef)
				convey.So(items[0].Next.Links.Self.HRef, convey.ShouldEqual, results[0].Next.Links.Self.HRef)
				convey.So(items[0].Next.Links.Taxonomy.HRef, convey.ShouldEqual, results[0].Next.Links.Taxonomy.HRef)
			})
		})

		convey.Convey("When the dataset links are nil", func() {
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

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Current.Links, convey.ShouldBeNil)
				convey.So(items[0].Next.Links, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dataset links are empty", func() {
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

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Current.Links, convey.ShouldResemble, &models.DatasetLinks{})
				convey.So(items[0].Next.Links, convey.ShouldResemble, &models.DatasetLinks{})
			})
		})

		convey.Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the datasets should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteDatasetsWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dataset updates", t, func() {
		convey.Convey("When the 'current' dataset links are unable to be parsed", func() {
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
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the 'next' dataset links are unable to be parsed", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
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

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDatasetsWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dataset updates", t, func() {
		convey.Convey("When the dataset links need rewriting", func() {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(items[0].Links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(items[0].Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				convey.So(items[0].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(items[0].Links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		convey.Convey("When the dataset links do not need rewriting", func() {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Links.AccessRights.HRef, convey.ShouldEqual, results[0].Current.Links.AccessRights.HRef)
				convey.So(items[0].Links.Editions.HRef, convey.ShouldEqual, results[0].Current.Links.Editions.HRef)
				convey.So(items[0].Links.LatestVersion.HRef, convey.ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				convey.So(items[0].Links.Self.HRef, convey.ShouldEqual, results[0].Current.Links.Self.HRef)
				convey.So(items[0].Links.Taxonomy.HRef, convey.ShouldEqual, results[0].Current.Links.Taxonomy.HRef)
			})
		})

		convey.Convey("When the dataset links are nil", func() {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Links, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dataset links are empty", func() {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "123")
				convey.So(items[0].Links, convey.ShouldResemble, &models.DatasetLinks{})
			})
		})

		convey.Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the datasets should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteDatasetsWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dataset updates", t, func() {
		convey.Convey("When the dataset links are unable to be parsed", func() {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDatasetWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a dataset update", t, func() {
		convey.Convey("When the dataset links need rewriting", func() {
			result := &models.DatasetUpdate{
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
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Current.Links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(item.Current.Links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(item.Current.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				convey.So(item.Current.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(item.Current.Links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				convey.So(item.Next.Links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(item.Next.Links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(item.Next.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				convey.So(item.Next.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(item.Next.Links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		convey.Convey("When the dataset links do not need rewriting", func() {
			result := &models.DatasetUpdate{
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
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Current.Links.AccessRights.HRef, convey.ShouldEqual, result.Current.Links.AccessRights.HRef)
				convey.So(item.Current.Links.Editions.HRef, convey.ShouldEqual, result.Current.Links.Editions.HRef)
				convey.So(item.Current.Links.LatestVersion.HRef, convey.ShouldEqual, result.Current.Links.LatestVersion.HRef)
				convey.So(item.Current.Links.Self.HRef, convey.ShouldEqual, result.Current.Links.Self.HRef)
				convey.So(item.Current.Links.Taxonomy.HRef, convey.ShouldEqual, result.Current.Links.Taxonomy.HRef)
				convey.So(item.Next.Links.AccessRights.HRef, convey.ShouldEqual, result.Next.Links.AccessRights.HRef)
				convey.So(item.Next.Links.Editions.HRef, convey.ShouldEqual, result.Next.Links.Editions.HRef)
				convey.So(item.Next.Links.LatestVersion.HRef, convey.ShouldEqual, result.Next.Links.LatestVersion.HRef)
				convey.So(item.Next.Links.Self.HRef, convey.ShouldEqual, result.Next.Links.Self.HRef)
				convey.So(item.Next.Links.Taxonomy.HRef, convey.ShouldEqual, result.Next.Links.Taxonomy.HRef)
			})
		})

		convey.Convey("When the dataset links are nil", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
				},
				Next: &models.Dataset{
					ID: "123",
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Current.Links, convey.ShouldBeNil)
				convey.So(item.Next.Links, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dataset links are empty", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
				Next: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Current.Links, convey.ShouldResemble, &models.DatasetLinks{})
				convey.So(item.Next.Links, convey.ShouldResemble, &models.DatasetLinks{})
			})
		})
	})
}

func TestRewriteDatasetWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a dataset update", t, func() {
		convey.Convey("When the 'current' dataset links are unable to be parsed", func() {
			result := &models.DatasetUpdate{
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
			}

			item, err := RewriteDatasetWithAuth(ctx, result, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the 'next' dataset links are unable to be parsed", func() {
			result := &models.DatasetUpdate{
				ID: "123",
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
			}

			item, err := RewriteDatasetWithAuth(ctx, result, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the dataset is empty", func() {
			result := &models.DatasetUpdate{}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then we should get a dataset not found error", func() {
				convey.So(err, convey.ShouldEqual, errs.ErrDatasetNotFound)
				convey.So(item, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dataset is nil", func() {
			item, err := RewriteDatasetWithAuth(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then we should get a dataset not found error", func() {
				convey.So(err, convey.ShouldEqual, errs.ErrDatasetNotFound)
				convey.So(item, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a dataset update", t, func() {
		convey.Convey("When the dataset links need rewriting", func() {
			result := &models.DatasetUpdate{
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
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(item.Links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(item.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				convey.So(item.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(item.Links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		convey.Convey("When the dataset links do not need rewriting", func() {
			result := &models.DatasetUpdate{
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
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Links.AccessRights.HRef, convey.ShouldEqual, result.Current.Links.AccessRights.HRef)
				convey.So(item.Links.Editions.HRef, convey.ShouldEqual, result.Current.Links.Editions.HRef)
				convey.So(item.Links.LatestVersion.HRef, convey.ShouldEqual, result.Current.Links.LatestVersion.HRef)
				convey.So(item.Links.Self.HRef, convey.ShouldEqual, result.Current.Links.Self.HRef)
				convey.So(item.Links.Taxonomy.HRef, convey.ShouldEqual, result.Current.Links.Taxonomy.HRef)
			})
		})

		convey.Convey("When the dataset links are nil", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
				},
				Next: &models.Dataset{
					ID: "123",
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Links, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dataset links are empty", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
				Next: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "123")
				convey.So(item.Links, convey.ShouldResemble, &models.DatasetLinks{})
			})
		})
	})
}

func TestRewriteDatasetWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a dataset update", t, func() {
		convey.Convey("When the dataset links are unable to be parsed", func() {
			result := &models.DatasetUpdate{
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
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the dataset is empty", func() {
			result := &models.DatasetUpdate{}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then we should get a dataset not found error", func() {
				convey.So(err, convey.ShouldEqual, errs.ErrDatasetNotFound)
				convey.So(item, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dataset is nil", func() {
			item, err := RewriteDatasetWithoutAuth(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then we should get a dataset not found error", func() {
				convey.So(err, convey.ShouldEqual, errs.ErrDatasetNotFound)
				convey.So(item, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of dataset links", t, func() {
		convey.Convey("When the dataset links need rewriting", func() {
			links := &models.DatasetLinks{
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
			}

			err := RewriteDatasetLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		convey.Convey("When the dataset links do not need rewriting", func() {
			links := &models.DatasetLinks{
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
			}

			err := RewriteDatasetLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(links.Editions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions")
				convey.So(links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/123")
				convey.So(links.Taxonomy.HRef, convey.ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		convey.Convey("When the dataset links are empty", func() {
			links := &models.DatasetLinks{}

			err := RewriteDatasetLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, &models.DatasetLinks{})
			})
		})

		convey.Convey("When the dataset links are nil", func() {
			err := RewriteDatasetLinks(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of dataset links", t, func() {
		convey.Convey("When the dataset links are unable to be parsed", func() {
			links := &models.DatasetLinks{
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
			}

			err := RewriteDatasetLinks(ctx, links, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensions_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dimensions", t, func() {
		convey.Convey("When the dimension links need rewriting", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Options: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Options: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].Label, convey.ShouldEqual, "Aggregate")
				convey.So(items[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(items[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(items[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[0].Name, convey.ShouldEqual, "aggregate")
				convey.So(items[1].Label, convey.ShouldEqual, "Geography")
				convey.So(items[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(items[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(items[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[1].Name, convey.ShouldEqual, "geography")
			})
		})

		convey.Convey("When the dimension links do not need rewriting", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Options: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/uk-only",
							ID:   "uk-only",
						},
						Options: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].Label, convey.ShouldEqual, "Aggregate")
				convey.So(items[0].Links.CodeList.HRef, convey.ShouldEqual, results[0].Links.CodeList.HRef)
				convey.So(items[0].Links.Options.HRef, convey.ShouldEqual, results[0].Links.Options.HRef)
				convey.So(items[0].Links.Version.HRef, convey.ShouldEqual, results[0].Links.Version.HRef)
				convey.So(items[0].Name, convey.ShouldEqual, "aggregate")
				convey.So(items[1].Label, convey.ShouldEqual, "Geography")
				convey.So(items[1].Links.CodeList.HRef, convey.ShouldEqual, results[1].Links.CodeList.HRef)
				convey.So(items[1].Links.Options.HRef, convey.ShouldEqual, results[1].Links.Options.HRef)
				convey.So(items[1].Links.Version.HRef, convey.ShouldEqual, results[1].Links.Version.HRef)
				convey.So(items[1].Name, convey.ShouldEqual, "geography")
			})
		})

		convey.Convey("When each dimension needs its link rewritten", func() {
			results := []models.Dimension{
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "https://oldhost:1000/code-lists/mmm-yy",
					ID:   "mmm-yy",
					Name: "time",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "https://oldhost:1000/code-lists/uk-only",
					ID:   "uk-only",
					Name: "geography",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
					Name: "aggregate",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				convey.So(items[1].HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(items[2].HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
			})
		})

		convey.Convey("When each dimemsion doesn't need its link rewritten", func() {
			results := []models.Dimension{
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22400/code-lists/mmm-yy",
					ID:   "mmm-yy",
					Name: "time",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22400/code-lists/uk-only",
					ID:   "uk-only",
					Name: "geography",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
					Name: "aggregate",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].HRef, convey.ShouldEqual, results[0].HRef)
				convey.So(items[1].HRef, convey.ShouldEqual, results[1].HRef)
				convey.So(items[2].HRef, convey.ShouldEqual, results[2].HRef)
			})
		})

		convey.Convey("When the dimension links are empty", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{},
					Name:  "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{},
					Name:  "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].Label, convey.ShouldEqual, "Aggregate")
				convey.So(items[0].Links, convey.ShouldResemble, models.DimensionLink{})
				convey.So(items[0].Name, convey.ShouldEqual, "aggregate")
				convey.So(items[1].Label, convey.ShouldEqual, "Geography")
				convey.So(items[1].Links, convey.ShouldResemble, models.DimensionLink{})
				convey.So(items[1].Name, convey.ShouldEqual, "geography")
			})
		})

		convey.Convey("When the dimensions are nil", func() {
			items, err := RewriteDimensions(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the dimensions should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dimensions are empty", func() {
			items, err := RewriteDimensions(ctx, []models.Dimension{}, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldResemble, []models.Dimension{})
			})
		})
	})
}

func TestRewriteDimensions_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dimensions", t, func() {
		convey.Convey("When the dimension links are unable to be parsed", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Options: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Options: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, nil, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of dimension links", t, func() {
		convey.Convey("When the dimension links need rewriting", func() {
			links := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the dimension links do not need rewriting", func() {
			links := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the dimension links are empty", func() {
			links := models.DimensionLink{}

			err := RewriteDimensionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, models.DimensionLink{})
			})
		})

		convey.Convey("When the dimension links are nil", func() {
			err := RewriteDimensionLinks(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteDimensionLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of dimension links", t, func() {
		convey.Convey("When the Code List link is unable to be parsed", func() {
			links := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the Options link is unable to be parsed", func() {
			links := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the Version link is unable to be parsed", func() {
			links := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewritePublicDimensionOptions_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of public dimension options", t, func() {
		convey.Convey("When the public dimension options need rewriting", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/uk-only/codes/K02000001",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
				{
					Label: "Time",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy/codes/Apr-00",
							ID:   "time",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "time",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].Label, convey.ShouldEqual, "Aggregate")
				convey.So(items[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(items[0].Links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1A0")
				convey.So(items[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[0].Name, convey.ShouldEqual, "aggregate")
				convey.So(items[1].Label, convey.ShouldEqual, "Geography")
				convey.So(items[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(items[1].Links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only/codes/K02000001")
				convey.So(items[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[1].Name, convey.ShouldEqual, "geography")
				convey.So(items[2].Label, convey.ShouldEqual, "Time")
				convey.So(items[2].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				convey.So(items[2].Links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy/codes/Apr-00")
				convey.So(items[2].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[2].Name, convey.ShouldEqual, "time")
			})
		})

		convey.Convey("When the public dimension options do not need rewriting", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/uk-only/codes/K02000001",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
				{
					Label: "Time",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy/codes/Apr-00",
							ID:   "time",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "time",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].Label, convey.ShouldEqual, "Aggregate")
				convey.So(items[0].Links.CodeList.HRef, convey.ShouldEqual, results[0].Links.CodeList.HRef)
				convey.So(items[0].Links.Code.HRef, convey.ShouldEqual, results[0].Links.Code.HRef)
				convey.So(items[0].Links.Version.HRef, convey.ShouldEqual, results[0].Links.Version.HRef)
				convey.So(items[0].Name, convey.ShouldEqual, "aggregate")
				convey.So(items[1].Label, convey.ShouldEqual, "Geography")
				convey.So(items[1].Links.CodeList.HRef, convey.ShouldEqual, results[1].Links.CodeList.HRef)
				convey.So(items[1].Links.Code.HRef, convey.ShouldEqual, results[1].Links.Code.HRef)
				convey.So(items[1].Links.Version.HRef, convey.ShouldEqual, results[1].Links.Version.HRef)
				convey.So(items[1].Name, convey.ShouldEqual, "geography")
				convey.So(items[2].Label, convey.ShouldEqual, "Time")
				convey.So(items[2].Links.CodeList.HRef, convey.ShouldEqual, results[2].Links.CodeList.HRef)
				convey.So(items[2].Links.Code.HRef, convey.ShouldEqual, results[2].Links.Code.HRef)
				convey.So(items[2].Links.Version.HRef, convey.ShouldEqual, results[2].Links.Version.HRef)
				convey.So(items[2].Name, convey.ShouldEqual, "time")
			})
		})

		convey.Convey("When the public dimension options are nil", func() {
			items, err := RewritePublicDimensionOptions(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the public dimension options should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the public dimension options are empty", func() {
			items, err := RewritePublicDimensionOptions(ctx, []*models.PublicDimensionOption{}, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldResemble, []*models.PublicDimensionOption{})
			})
		})

		convey.Convey("When the public dimension option links are empty", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{},
					Name:  "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{},
					Name:  "geography",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].Label, convey.ShouldEqual, "Aggregate")
				convey.So(items[0].Links, convey.ShouldResemble, models.DimensionOptionLinks{})
				convey.So(items[0].Name, convey.ShouldEqual, "aggregate")
				convey.So(items[1].Label, convey.ShouldEqual, "Geography")
				convey.So(items[1].Links, convey.ShouldResemble, models.DimensionOptionLinks{})
				convey.So(items[1].Name, convey.ShouldEqual, "geography")
			})
		})
	})
}

func TestRewritePublicDimensionOptions_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of public dimension options", t, func() {
		convey.Convey("When the public dimension options are unable to be parsed", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/uk-only/codes/K02000001",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, nil, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionOptions_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dimension options", t, func() {
		convey.Convey("When the dimension options need rewriting", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label: "01.1 Food",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
				{
					Label: "Mar-02",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy/codes/Mar-02",
							ID:   "Mar-02",
						},
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_Mar-02",
					Option: "Mar-02",
				},
			}

			err := RewriteDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(results[0].Label, convey.ShouldEqual, "May-89")
				convey.So(results[0].Links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy/codes/May-89")
				convey.So(results[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				convey.So(results[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[0].Name, convey.ShouldEqual, "time")
				convey.So(results[0].NodeID, convey.ShouldEqual, "_37abc12d_time_May-89")
				convey.So(results[0].Option, convey.ShouldEqual, "May-89")
				convey.So(results[1].Label, convey.ShouldEqual, "01.1 Food")
				convey.So(results[1].Links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				convey.So(results[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(results[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[1].Name, convey.ShouldEqual, "aggregate")
				convey.So(results[1].NodeID, convey.ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				convey.So(results[1].Option, convey.ShouldEqual, "cpih1dim1G10100")
				convey.So(results[2].Label, convey.ShouldEqual, "Mar-02")
				convey.So(results[2].Links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy/codes/Mar-02")
				convey.So(results[2].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				convey.So(results[2].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[2].Name, convey.ShouldEqual, "time")
				convey.So(results[2].NodeID, convey.ShouldEqual, "_37abc12d_time_Mar-02")
				convey.So(results[2].Option, convey.ShouldEqual, "Mar-02")
			})
		})

		convey.Convey("When the dimension options do not need rewriting", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label: "01.1 Food",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
				{
					Label: "Mar-02",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy/codes/Mar-02",
							ID:   "Mar-02",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_Mar-02",
					Option: "Mar-02",
				},
			}

			err := RewriteDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(results[0].Label, convey.ShouldEqual, "May-89")
				convey.So(results[0].Links.Code.HRef, convey.ShouldEqual, results[0].Links.Code.HRef)
				convey.So(results[0].Links.CodeList.HRef, convey.ShouldEqual, results[0].Links.CodeList.HRef)
				convey.So(results[0].Links.Version.HRef, convey.ShouldEqual, results[0].Links.Version.HRef)
				convey.So(results[0].Name, convey.ShouldEqual, "time")
				convey.So(results[0].NodeID, convey.ShouldEqual, "_37abc12d_time_May-89")
				convey.So(results[0].Option, convey.ShouldEqual, "May-89")
				convey.So(results[1].Label, convey.ShouldEqual, "01.1 Food")
				convey.So(results[1].Links.Code.HRef, convey.ShouldEqual, results[1].Links.Code.HRef)
				convey.So(results[1].Links.CodeList.HRef, convey.ShouldEqual, results[1].Links.CodeList.HRef)
				convey.So(results[1].Links.Version.HRef, convey.ShouldEqual, results[1].Links.Version.HRef)
				convey.So(results[1].Name, convey.ShouldEqual, "aggregate")
				convey.So(results[1].NodeID, convey.ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				convey.So(results[1].Option, convey.ShouldEqual, "cpih1dim1G10100")
				convey.So(results[2].Label, convey.ShouldEqual, "Mar-02")
				convey.So(results[2].Links.Code.HRef, convey.ShouldEqual, results[2].Links.Code.HRef)
				convey.So(results[2].Links.CodeList.HRef, convey.ShouldEqual, results[2].Links.CodeList.HRef)
				convey.So(results[2].Links.Version.HRef, convey.ShouldEqual, results[2].Links.Version.HRef)
				convey.So(results[2].Name, convey.ShouldEqual, "time")
				convey.So(results[2].NodeID, convey.ShouldEqual, "_37abc12d_time_Mar-02")
				convey.So(results[2].Option, convey.ShouldEqual, "Mar-02")
			})
		})

		convey.Convey("When the dimension options are nil", func() {
			err := RewriteDimensionOptions(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the dimension options should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dimension options are empty", func() {
			err := RewriteDimensionOptions(ctx, []*models.DimensionOption{}, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the dimension option links are empty", func() {
			results := []*models.DimensionOption{
				{
					Label:  "May-89",
					Links:  models.DimensionOptionLinks{},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label:  "01.1 Food",
					Links:  models.DimensionOptionLinks{},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
			}

			err := RewriteDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(results[0].Label, convey.ShouldEqual, "May-89")
				convey.So(results[0].Links, convey.ShouldResemble, models.DimensionOptionLinks{})
				convey.So(results[0].Name, convey.ShouldEqual, "time")
				convey.So(results[0].NodeID, convey.ShouldEqual, "_37abc12d_time_May-89")
				convey.So(results[0].Option, convey.ShouldEqual, "May-89")
				convey.So(results[1].Label, convey.ShouldEqual, "01.1 Food")
				convey.So(results[1].Links, convey.ShouldResemble, models.DimensionOptionLinks{})
				convey.So(results[1].Name, convey.ShouldEqual, "aggregate")
				convey.So(results[1].NodeID, convey.ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				convey.So(results[1].Option, convey.ShouldEqual, "cpih1dim1G10100")
			})
		})
	})
}

func TestRewriteDimensionOptions_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of dimension options", t, func() {
		convey.Convey("When the dimension options are unable to be parsed", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label: "01.1 Food",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
			}

			err := RewriteDimensionOptions(ctx, results, nil, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionOptionLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of dimension option links", t, func() {
		convey.Convey("When the dimension option links need rewriting", func() {
			links := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				convey.So(links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the dimension option links do not need rewriting", func() {
			links := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Code.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				convey.So(links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the dimension option links are empty", func() {
			links := models.DimensionOptionLinks{}

			err := RewriteDimensionOptionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, models.DimensionOptionLinks{})
			})
		})

		convey.Convey("When the dimension option links are nil", func() {
			err := RewriteDimensionOptionLinks(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteDimensionOptionLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of dimension option links", t, func() {
		convey.Convey("When the Code link is unable to be parsed", func() {
			links := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the CodeList link is unable to be parsed", func() {
			links := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the Version link is unable to be parsed", func() {
			links := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &links, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionsWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of edition updates", t, func() {
		convey.Convey("When the edition update links need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(items[0].Current.Edition, convey.ShouldEqual, "time-series")
				convey.So(items[0].Current.Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(items[0].Current.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[0].Current.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(items[0].Current.Links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				convey.So(items[0].Current.State, convey.ShouldEqual, "edition-confirmed")
				convey.So(items[0].Next.Edition, convey.ShouldEqual, "time-series")
				convey.So(items[0].Next.Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(items[0].Next.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[0].Next.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(items[0].Next.Links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				convey.So(items[0].Next.State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update links do not need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, results[0].ID)
				convey.So(items[0].Current.Edition, convey.ShouldEqual, results[0].Current.Edition)
				convey.So(items[0].Current.Links.Dataset.HRef, convey.ShouldEqual, results[0].Current.Links.Dataset.HRef)
				convey.So(items[0].Current.Links.LatestVersion.HRef, convey.ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				convey.So(items[0].Current.Links.Self.HRef, convey.ShouldEqual, results[0].Current.Links.Self.HRef)
				convey.So(items[0].Current.Links.Versions.HRef, convey.ShouldEqual, results[0].Current.Links.Versions.HRef)
				convey.So(items[0].Current.State, convey.ShouldEqual, results[0].Current.State)
				convey.So(items[0].Next.Edition, convey.ShouldEqual, results[0].Next.Edition)
				convey.So(items[0].Next.Links.Dataset.HRef, convey.ShouldEqual, results[0].Next.Links.Dataset.HRef)
				convey.So(items[0].Next.Links.LatestVersion.HRef, convey.ShouldEqual, results[0].Next.Links.LatestVersion.HRef)
				convey.So(items[0].Next.Links.Self.HRef, convey.ShouldEqual, results[0].Next.Links.Self.HRef)
				convey.So(items[0].Next.Links.Versions.HRef, convey.ShouldEqual, results[0].Next.Links.Versions.HRef)
				convey.So(items[0].Next.State, convey.ShouldEqual, results[0].Next.State)
			})
		})

		convey.Convey("When the edition updates are nil", func() {
			items, err := RewriteEditionsWithAuth(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then the edition updates should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the edition updates are empty", func() {
			items, err := RewriteEditionsWithAuth(ctx, []*models.EditionUpdate{}, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldResemble, []*models.EditionUpdate{})
			})
		})

		convey.Convey("When the edition update links are empty", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(items[0].Current.Links, convey.ShouldResemble, &models.EditionUpdateLinks{})
				convey.So(items[0].Current.State, convey.ShouldEqual, "edition-confirmed")
				convey.So(items[0].Next.Links, convey.ShouldResemble, &models.EditionUpdateLinks{})
				convey.So(items[0].Next.State, convey.ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionsWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of edition updates", t, func() {
		convey.Convey("When the 'current' edition update links are unable to be parsed", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the 'next' edition update links are unable to be parsed", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionsWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of edition updates", t, func() {
		convey.Convey("When the edition update links need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(items[0].Edition, convey.ShouldEqual, "time-series")
				convey.So(items[0].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(items[0].Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[0].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(items[0].Links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				convey.So(items[0].State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update links do not need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, results[0].ID)
				convey.So(items[0].Edition, convey.ShouldEqual, results[0].Current.Edition)
				convey.So(items[0].Links.Dataset.HRef, convey.ShouldEqual, results[0].Current.Links.Dataset.HRef)
				convey.So(items[0].Links.LatestVersion.HRef, convey.ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				convey.So(items[0].Links.Self.HRef, convey.ShouldEqual, results[0].Current.Links.Self.HRef)
				convey.So(items[0].Links.Versions.HRef, convey.ShouldEqual, results[0].Current.Links.Versions.HRef)
				convey.So(items[0].State, convey.ShouldEqual, results[0].Current.State)
			})
		})

		convey.Convey("When the edition updates are nil", func() {
			items, err := RewriteEditionsWithoutAuth(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then an empty list should be returned", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldResemble, []*models.Edition{})
			})
		})

		convey.Convey("When the edition updates are empty", func() {
			items, err := RewriteEditionsWithoutAuth(ctx, []*models.EditionUpdate{}, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldResemble, []*models.Edition{})
			})
		})

		convey.Convey("When the edition update links are empty", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items[0].ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(items[0].Links, convey.ShouldResemble, &models.EditionUpdateLinks{})
				convey.So(items[0].State, convey.ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionsWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of edition updates", t, func() {
		convey.Convey("When the edition update links are unable to be parsed", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given an edition update", t, func() {
		convey.Convey("When the edition update links need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(item.Current.Edition, convey.ShouldEqual, "time-series")
				convey.So(item.Current.Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(item.Current.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(item.Current.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(item.Current.Links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				convey.So(item.Current.State, convey.ShouldEqual, "edition-confirmed")
				convey.So(item.Next.Edition, convey.ShouldEqual, "time-series")
				convey.So(item.Next.Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(item.Next.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2")
				convey.So(item.Next.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(item.Next.Links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				convey.So(item.Next.State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update links do not need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, result.ID)
				convey.So(item.Current.Edition, convey.ShouldEqual, result.Current.Edition)
				convey.So(item.Current.Links.Dataset.HRef, convey.ShouldEqual, result.Current.Links.Dataset.HRef)
				convey.So(item.Current.Links.LatestVersion.HRef, convey.ShouldEqual, result.Current.Links.LatestVersion.HRef)
				convey.So(item.Current.Links.Self.HRef, convey.ShouldEqual, result.Current.Links.Self.HRef)
				convey.So(item.Current.Links.Versions.HRef, convey.ShouldEqual, result.Current.Links.Versions.HRef)
				convey.So(item.Current.State, convey.ShouldEqual, result.Current.State)
				convey.So(item.Next.Edition, convey.ShouldEqual, result.Next.Edition)
				convey.So(item.Next.Links.Dataset.HRef, convey.ShouldEqual, result.Next.Links.Dataset.HRef)
				convey.So(item.Next.Links.LatestVersion.HRef, convey.ShouldEqual, result.Next.Links.LatestVersion.HRef)
				convey.So(item.Next.Links.Self.HRef, convey.ShouldEqual, result.Next.Links.Self.HRef)
				convey.So(item.Next.Links.Versions.HRef, convey.ShouldEqual, result.Next.Links.Versions.HRef)
				convey.So(item.Next.State, convey.ShouldEqual, result.Next.State)
			})
		})

		convey.Convey("When the edition update links are empty", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(item.Current.Links, convey.ShouldResemble, &models.EditionUpdateLinks{})
				convey.So(item.Current.State, convey.ShouldEqual, "edition-confirmed")
				convey.So(item.Next.Links, convey.ShouldResemble, &models.EditionUpdateLinks{})
				convey.So(item.Next.State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update links are nil", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(item.Current.Links, convey.ShouldBeNil)
				convey.So(item.Current.State, convey.ShouldEqual, "edition-confirmed")
				convey.So(item.Next.Links, convey.ShouldBeNil)
				convey.So(item.Next.State, convey.ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given an edition update", t, func() {
		convey.Convey("When the 'current' edition update links are unable to be parsed", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the 'next' edition update links are unable to be parsed", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the edition update is nil", func() {
			item, err := RewriteEditionWithAuth(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then an edition not found error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "edition not found")
			})
		})
	})
}

func TestRewriteEditionWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given an edition update", t, func() {
		convey.Convey("When the edition update links need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(item.Edition, convey.ShouldEqual, "time-series")
				convey.So(item.Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(item.Links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(item.Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(item.Links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				convey.So(item.State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update links do not need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, result.ID)
				convey.So(item.Edition, convey.ShouldEqual, result.Current.Edition)
				convey.So(item.Links.Dataset.HRef, convey.ShouldEqual, result.Current.Links.Dataset.HRef)
				convey.So(item.Links.LatestVersion.HRef, convey.ShouldEqual, result.Current.Links.LatestVersion.HRef)
				convey.So(item.Links.Self.HRef, convey.ShouldEqual, result.Current.Links.Self.HRef)
				convey.So(item.Links.Versions.HRef, convey.ShouldEqual, result.Current.Links.Versions.HRef)
				convey.So(item.State, convey.ShouldEqual, result.Current.State)
			})
		})

		convey.Convey("When the edition update links are empty", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(item.Links, convey.ShouldResemble, &models.EditionUpdateLinks{})
				convey.So(item.State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update links are nil", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item.ID, convey.ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				convey.So(item.Links, convey.ShouldBeNil)
				convey.So(item.State, convey.ShouldEqual, "edition-confirmed")
			})
		})

		convey.Convey("When the edition update is empty", func() {
			item, err := RewriteEditionWithoutAuth(ctx, &models.EditionUpdate{}, datasetLinksBuilder)

			convey.Convey("Then nothing should be returned", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the edition update 'current' is nil", func() {
			item, err := RewriteEditionWithoutAuth(ctx, &models.EditionUpdate{
				Current: nil,
				Next:    &models.Edition{},
			}, datasetLinksBuilder)

			convey.Convey("Then nothing should be returned", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(item, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteEditionWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given an edition update", t, func() {
		convey.Convey("When the edition update links are unable to be parsed", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the edition update is nil", func() {
			item, err := RewriteEditionWithoutAuth(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then an edition not found error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(item, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "edition not found")
			})
		})
	})
}

func TestRewriteEditionLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of edition update links", t, func() {
		convey.Convey("When the edition update links need rewriting", func() {
			links := &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				LatestVersion: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Versions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
				},
			}

			err := RewriteEditionLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
			})
		})

		convey.Convey("When the edition update links do not need rewriting", func() {
			links := &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
					ID:   "cpih01",
				},
				LatestVersion: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Versions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
				},
			}

			err := RewriteEditionLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(links.LatestVersion.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(links.Versions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
			})
		})

		convey.Convey("When the edition update links are empty", func() {
			links := &models.EditionUpdateLinks{}

			err := RewriteEditionLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, &models.EditionUpdateLinks{})
			})
		})

		convey.Convey("When the edition update links are nil", func() {
			err := RewriteEditionLinks(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteEditionLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of edition update links", t, func() {
		convey.Convey("When the edition update links are unable to be parsed", func() {
			links := &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				LatestVersion: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Versions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
				},
			}

			err := RewriteEditionLinks(ctx, links, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteMetadataLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of metadata links", t, func() {
		convey.Convey("When the metadata links need rewriting", func() {
			links := &models.MetadataLinks{
				AccessRights: &models.LinkObject{
					HRef: "https://oldhost:1000/accessrights",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.AccessRights.HRef, convey.ShouldEqual, "https://oldhost:1000/accessrights")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Version.ID, convey.ShouldEqual, "1")
				convey.So(links.WebsiteVersion.HRef, convey.ShouldEqual, "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the metadata links do not need rewriting", func() {
			links := &models.MetadataLinks{
				AccessRights: &models.LinkObject{
					HRef: "http://localhost:22000/accessrights",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.AccessRights.HRef, convey.ShouldEqual, "http://localhost:22000/accessrights")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Version.ID, convey.ShouldEqual, "1")
				convey.So(links.WebsiteVersion.HRef, convey.ShouldEqual, "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the metadata links are empty", func() {
			links := &models.MetadataLinks{}

			err := RewriteMetadataLinks(ctx, links, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, &models.MetadataLinks{})
			})
		})

		convey.Convey("When the metadata links are nil", func() {
			err := RewriteMetadataLinks(ctx, nil, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the metadata links are missing", func() {
			err := RewriteMetadataLinks(ctx, &models.MetadataLinks{}, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteMetadataLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of metadata links", t, func() {
		convey.Convey("When the Self link is unable to be parsed", func() {
			links := &models.MetadataLinks{
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the Version link is unable to be parsed", func() {
			links := &models.MetadataLinks{
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the WebsiteVersion link is unable to be parsed", func() {
			links := &models.MetadataLinks{
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, datasetLinksBuilder, websiteLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteVersions_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of versions", t, func() {
		convey.Convey("When the version and dimension links need rewriting", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/53",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
				},
				{
					ID:        "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   52,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/52",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(items[0].ID, convey.ShouldEqual, "cf4b2196-3548-4bd5-8288-92fe4ca06327")
				convey.So(items[0].DatasetID, convey.ShouldEqual, "cpih01")
				convey.So(items[0].Edition, convey.ShouldEqual, "time-series")
				convey.So(items[0].Version, convey.ShouldEqual, 53)
				convey.So(items[0].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(items[0].Links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(items[0].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/53")
				convey.So(items[0].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(items[0].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(items[0].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[0].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(items[0].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(items[0].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")

				convey.So(items[1].ID, convey.ShouldEqual, "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b")
				convey.So(items[1].DatasetID, convey.ShouldEqual, "cpih01")
				convey.So(items[1].Edition, convey.ShouldEqual, "time-series")
				convey.So(items[1].Version, convey.ShouldEqual, 52)
				convey.So(items[1].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(items[1].Links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(items[1].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/52")
				convey.So(items[1].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(items[1].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(items[1].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(items[1].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(items[1].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(items[1].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the version and dimension links do not need rewriting", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/53",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
				},
				{
					ID:        "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   52,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/52",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(items[0].ID, convey.ShouldEqual, results[0].ID)
				convey.So(items[0].DatasetID, convey.ShouldEqual, results[0].DatasetID)
				convey.So(items[0].Edition, convey.ShouldEqual, results[0].Edition)
				convey.So(items[0].Version, convey.ShouldEqual, results[0].Version)
				convey.So(items[0].Links.Dataset.HRef, convey.ShouldEqual, results[0].Links.Dataset.HRef)
				convey.So(items[0].Links.Edition.HRef, convey.ShouldEqual, results[0].Links.Edition.HRef)
				convey.So(items[0].Links.Self.HRef, convey.ShouldEqual, results[0].Links.Self.HRef)
				convey.So(items[0].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, results[0].Dimensions[0].Links.CodeList.HRef)
				convey.So(items[0].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, results[0].Dimensions[0].Links.Options.HRef)
				convey.So(items[0].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, results[0].Dimensions[0].Links.Version.HRef)
				convey.So(items[0].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, results[0].Dimensions[1].Links.CodeList.HRef)
				convey.So(items[0].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, results[0].Dimensions[1].Links.Options.HRef)
				convey.So(items[0].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, results[0].Dimensions[1].Links.Version.HRef)

				convey.So(items[1].ID, convey.ShouldEqual, results[1].ID)
				convey.So(items[1].DatasetID, convey.ShouldEqual, results[1].DatasetID)
				convey.So(items[1].Edition, convey.ShouldEqual, results[1].Edition)
				convey.So(items[1].Version, convey.ShouldEqual, results[1].Version)
				convey.So(items[1].Links.Dataset.HRef, convey.ShouldEqual, results[1].Links.Dataset.HRef)
				convey.So(items[1].Links.Edition.HRef, convey.ShouldEqual, results[1].Links.Edition.HRef)
				convey.So(items[1].Links.Self.HRef, convey.ShouldEqual, results[1].Links.Self.HRef)
				convey.So(items[1].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, results[1].Dimensions[0].Links.CodeList.HRef)
				convey.So(items[1].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, results[1].Dimensions[0].Links.Options.HRef)
				convey.So(items[1].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, results[1].Dimensions[0].Links.Version.HRef)
				convey.So(items[1].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, results[1].Dimensions[1].Links.CodeList.HRef)
				convey.So(items[1].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, results[1].Dimensions[1].Links.Options.HRef)
				convey.So(items[1].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, results[1].Dimensions[1].Links.Version.HRef)
			})
		})

		convey.Convey("When the version and dimension links are empty", func() {
			results := []models.Version{
				{
					ID:         "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID:  "cpih01",
					Edition:    "time-series",
					Version:    53,
					Links:      &models.VersionLinks{},
					Dimensions: []models.Dimension{},
				},
				{
					ID:         "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID:  "cpih01",
					Edition:    "time-series",
					Version:    52,
					Links:      &models.VersionLinks{},
					Dimensions: []models.Dimension{},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(items[0].ID, convey.ShouldEqual, results[0].ID)
				convey.So(items[0].DatasetID, convey.ShouldEqual, results[0].DatasetID)
				convey.So(items[0].Edition, convey.ShouldEqual, results[0].Edition)
				convey.So(items[0].Version, convey.ShouldEqual, results[0].Version)
				convey.So(items[0].Links, convey.ShouldResemble, &models.VersionLinks{})
				convey.So(items[0].Dimensions, convey.ShouldResemble, []models.Dimension{})

				convey.So(items[1].ID, convey.ShouldEqual, results[1].ID)
				convey.So(items[1].DatasetID, convey.ShouldEqual, results[1].DatasetID)
				convey.So(items[1].Edition, convey.ShouldEqual, results[1].Edition)
				convey.So(items[1].Version, convey.ShouldEqual, results[1].Version)
				convey.So(items[1].Links, convey.ShouldResemble, &models.VersionLinks{})
				convey.So(items[1].Dimensions, convey.ShouldResemble, []models.Dimension{})
			})
		})

		convey.Convey("When the version and dimension links are nil", func() {
			results := []models.Version{
				{
					ID:         "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID:  "cpih01",
					Edition:    "time-series",
					Version:    53,
					Links:      nil,
					Dimensions: nil,
				},
				{
					ID:         "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID:  "cpih01",
					Edition:    "time-series",
					Version:    52,
					Links:      nil,
					Dimensions: nil,
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(items[0].ID, convey.ShouldEqual, results[0].ID)
				convey.So(items[0].DatasetID, convey.ShouldEqual, results[0].DatasetID)
				convey.So(items[0].Edition, convey.ShouldEqual, results[0].Edition)
				convey.So(items[0].Version, convey.ShouldEqual, results[0].Version)
				convey.So(items[0].Links, convey.ShouldBeNil)
				convey.So(items[0].Dimensions, convey.ShouldBeNil)

				convey.So(items[1].ID, convey.ShouldEqual, results[1].ID)
				convey.So(items[1].DatasetID, convey.ShouldEqual, results[1].DatasetID)
				convey.So(items[1].Edition, convey.ShouldEqual, results[1].Edition)
				convey.So(items[1].Version, convey.ShouldEqual, results[1].Version)
				convey.So(items[1].Links, convey.ShouldBeNil)
				convey.So(items[1].Dimensions, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the versions are empty", func() {
			results := []models.Version{}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then the versions should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(items, convey.ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteVersions_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of versions", t, func() {
		convey.Convey("When the version links are unable to be parsed", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/53",
						},
					},
				},
				{
					ID:        "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   52,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/52",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, nil, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the dimension links are unable to be parsed", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Dimensions: []models.Dimension{
						{
							HRef:  "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
								},
								Version: models.LinkObject{
									HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(items, convey.ShouldBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteVersionLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of version links", t, func() {
		convey.Convey("When the version links need rewriting", func() {
			links := &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
					ID:   "time-series",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
				Spatial: &models.LinkObject{
					HRef: "https://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
			}

			err := RewriteVersionLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Spatial.HRef, convey.ShouldEqual, "https://oldhost:1000/spatial")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Version.ID, convey.ShouldEqual, "1")
			})
		})

		convey.Convey("When the version links do not need rewriting", func() {
			links := &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
					ID:   "cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
					ID:   "time-series",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
			}

			err := RewriteVersionLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(links.Version.ID, convey.ShouldEqual, "1")
			})
		})

		convey.Convey("When the version links are empty", func() {
			links := &models.VersionLinks{}

			err := RewriteVersionLinks(ctx, links, datasetLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, &models.VersionLinks{})
			})
		})

		convey.Convey("When the version links are nil", func() {
			err := RewriteVersionLinks(ctx, nil, datasetLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteVersionLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of version links", t, func() {
		convey.Convey("When the version links are unable to be parsed", func() {
			links := &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
					ID:   "time-series",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
				Spatial: &models.LinkObject{
					HRef: "://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
			}

			err := RewriteVersionLinks(ctx, links, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteInstances_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of instances", t, func() {
		convey.Convey("When the instance links need rewriting", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "https://oldhost:1000/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/instances/1",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/1",
						},
						Version: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cantabular-flexible-default/editions/2021/versions/1",
							ID:   "1",
						},
					},
				},
				{
					CollectionID: "cpihtest-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "https://oldhost:1000/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/instances/2",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/2",
						},
						Version: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(results[0].CollectionID, convey.ShouldEqual, "cantabularflexibledefault-1")
				convey.So(results[0].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(results[0].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(results[0].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[0].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(results[0].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(results[0].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[0].Edition, convey.ShouldEqual, "2021")
				convey.So(results[0].InstanceID, convey.ShouldEqual, "1")
				convey.So(results[0].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default")
				convey.So(results[0].Links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(results[0].Links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021")
				convey.So(results[0].Links.Job.HRef, convey.ShouldEqual, "http://localhost:21800/jobs/1")
				convey.So(results[0].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/instances/1")
				convey.So(results[0].Links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial/1")
				convey.So(results[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1")

				convey.So(results[1].CollectionID, convey.ShouldEqual, "cpihtest-1")
				convey.So(results[1].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(results[1].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(results[1].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[1].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(results[1].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(results[1].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[1].Edition, convey.ShouldEqual, "time-series")
				convey.So(results[1].InstanceID, convey.ShouldEqual, "2")
				convey.So(results[1].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(results[1].Links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(results[1].Links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(results[1].Links.Job.HRef, convey.ShouldEqual, "http://localhost:21800/jobs/2")
				convey.So(results[1].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/instances/2")
				convey.So(results[1].Links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial/2")
				convey.So(results[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the instance links do not need rewriting", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "http://localhost:21800/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/1",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/1",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1",
							ID:   "1",
						},
					},
				},
				{
					CollectionID: "cpihtest-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "http://localhost:21800/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/2",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/2",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(results[0].CollectionID, convey.ShouldEqual, "cantabularflexibledefault-1")
				convey.So(results[0].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(results[0].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(results[0].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[0].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(results[0].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(results[0].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[0].Edition, convey.ShouldEqual, "2021")
				convey.So(results[0].InstanceID, convey.ShouldEqual, "1")
				convey.So(results[0].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default")
				convey.So(results[0].Links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(results[0].Links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021")
				convey.So(results[0].Links.Job.HRef, convey.ShouldEqual, "http://localhost:21800/jobs/1")
				convey.So(results[0].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/instances/1")
				convey.So(results[0].Links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial/1")
				convey.So(results[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1")

				convey.So(results[1].CollectionID, convey.ShouldEqual, "cpihtest-1")
				convey.So(results[1].Dimensions[0].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				convey.So(results[1].Dimensions[0].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				convey.So(results[1].Dimensions[0].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[1].Dimensions[1].Links.CodeList.HRef, convey.ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				convey.So(results[1].Dimensions[1].Links.Options.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				convey.So(results[1].Dimensions[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				convey.So(results[1].Edition, convey.ShouldEqual, "time-series")
				convey.So(results[1].InstanceID, convey.ShouldEqual, "2")
				convey.So(results[1].Links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(results[1].Links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(results[1].Links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(results[1].Links.Job.HRef, convey.ShouldEqual, "http://localhost:21800/jobs/2")
				convey.So(results[1].Links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/instances/2")
				convey.So(results[1].Links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial/2")
				convey.So(results[1].Links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the instance links are empty", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Edition:      "2021",
					InstanceID:   "1",
					Links:        &models.InstanceLinks{},
				},
				{
					CollectionID: "cpihtest-1",
					Edition:      "time-series",
					InstanceID:   "2",
					Links:        &models.InstanceLinks{},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.So(results[0].CollectionID, convey.ShouldEqual, "cantabularflexibledefault-1")
				convey.So(results[0].Edition, convey.ShouldEqual, "2021")
				convey.So(results[0].InstanceID, convey.ShouldEqual, "1")
				convey.So(results[0].Links, convey.ShouldResemble, &models.InstanceLinks{})

				convey.So(results[1].CollectionID, convey.ShouldEqual, "cpihtest-1")
				convey.So(results[1].Edition, convey.ShouldEqual, "time-series")
				convey.So(results[1].InstanceID, convey.ShouldEqual, "2")
				convey.So(results[1].Links, convey.ShouldResemble, &models.InstanceLinks{})
			})
		})

		convey.Convey("When the instances are empty", func() {
			results := []*models.Instance{}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder)

			convey.Convey("Then no error should be returned", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteInstances_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a list of instances", t, func() {
		convey.Convey("When the instance links are unable to be parsed", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Edition:      "2021",
					InstanceID:   "1",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "://oldhost:1000/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/instances/1",
						},
						Version: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default/editions/2021/versions/1",
							ID:   "1",
						},
					},
				},
				{
					CollectionID: "cpihtest-1",
					Edition:      "time-series",
					InstanceID:   "2",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "://oldhost:1000/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/instances/2",
						},
						Version: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the dimensions links are unable to be parsed", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "City",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/city",
							ID:   "city",
						},
						{
							Label: "Number of siblings (3 mappings)",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/siblings_3",
							ID:   "siblings_3",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
				},
				{
					CollectionID: "cpihtest-1",
					Dimensions: []models.Dimension{
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
							Name: "time",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
							Name: "geography",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
							Name: "aggregate",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteInstanceLinks_Success(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of instance links", t, func() {
		convey.Convey("When the instance links need rewriting", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "https://oldhost:1000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, datasetLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should be rewritten correctly", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(links.Job.HRef, convey.ShouldEqual, "http://localhost:21800/jobs/1")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/instances/1")
				convey.So(links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the instance links do not need rewriting", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "http://localhost:21800/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, datasetLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should remain the same", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links.Dataset.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01")
				convey.So(links.Dimensions.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				convey.So(links.Edition.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				convey.So(links.Job.HRef, convey.ShouldEqual, "http://localhost:21800/jobs/1")
				convey.So(links.Self.HRef, convey.ShouldEqual, "http://localhost:22000/instances/1")
				convey.So(links.Spatial.HRef, convey.ShouldEqual, "http://oldhost:1000/spatial")
				convey.So(links.Version.HRef, convey.ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		convey.Convey("When the instance links are empty", func() {
			links := &models.InstanceLinks{}

			err := RewriteInstanceLinks(ctx, links, datasetLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should remain empty", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(links, convey.ShouldResemble, &models.InstanceLinks{})
			})
		})

		convey.Convey("When the instance links are nil", func() {
			err := RewriteInstanceLinks(ctx, nil, datasetLinksBuilder, importLinksBuilder)

			convey.Convey("Then the links should remain nil", func() {
				convey.So(err, convey.ShouldBeNil)
			})
		})
	})
}

func TestRewriteInstanceLinks_Error(t *testing.T) {
	ctx := context.Background()
	convey.Convey("Given a set of instance links", t, func() {
		convey.Convey("When the instance links are unable to be parsed", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "://oldhost:1000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, nil, nil)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		convey.Convey("When the Job link is unable to be parsed", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "://oldhost:1000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://localhost:22000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, datasetLinksBuilder, importLinksBuilder)

			convey.Convey("Then a parsing error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}
