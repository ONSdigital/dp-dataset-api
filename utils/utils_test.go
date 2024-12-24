package utils

import (
	"context"
	"net/http"
	goURL "net/url"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
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
				So(items[0].Current.Links.AccessRights.HRef, ShouldEqual, results[0].Current.Links.AccessRights.HRef)
				So(items[0].Current.Links.Editions.HRef, ShouldEqual, results[0].Current.Links.Editions.HRef)
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Current.Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Current.Links.Taxonomy.HRef, ShouldEqual, results[0].Current.Links.Taxonomy.HRef)
				So(items[0].Next.Links.AccessRights.HRef, ShouldEqual, results[0].Next.Links.AccessRights.HRef)
				So(items[0].Next.Links.Editions.HRef, ShouldEqual, results[0].Next.Links.Editions.HRef)
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, results[0].Next.Links.LatestVersion.HRef)
				So(items[0].Next.Links.Self.HRef, ShouldEqual, results[0].Next.Links.Self.HRef)
				So(items[0].Next.Links.Taxonomy.HRef, ShouldEqual, results[0].Next.Links.Taxonomy.HRef)
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

			Convey("Then the links should remain nil", func() {
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

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links, ShouldResemble, &models.DatasetLinks{})
				So(items[0].Next.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithAuth(ctx, results, linksBuilder)

			Convey("Then the datasets should remain empty", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDatasetsWithoutAuth_Success(t *testing.T) {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(items[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links.AccessRights.HRef, ShouldEqual, results[0].Current.Links.AccessRights.HRef)
				So(items[0].Links.Editions.HRef, ShouldEqual, results[0].Current.Links.Editions.HRef)
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Links.Taxonomy.HRef, ShouldEqual, results[0].Current.Links.Taxonomy.HRef)
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links, ShouldBeNil)
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the datasets should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteDatasetsWithoutAuth_Error(t *testing.T) {
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

			items, err := RewriteDatasetsWithoutAuth(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDatasetWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		Convey("When the dataset links need rewriting", func() {
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

			item, err := RewriteDatasetWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(item.Current.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(item.Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(item.Current.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				So(item.Next.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(item.Next.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				So(item.Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(item.Next.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
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

			item, err := RewriteDatasetWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links.AccessRights.HRef, ShouldEqual, result.Current.Links.AccessRights.HRef)
				So(item.Current.Links.Editions.HRef, ShouldEqual, result.Current.Links.Editions.HRef)
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Current.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Current.Links.Taxonomy.HRef, ShouldEqual, result.Current.Links.Taxonomy.HRef)
				So(item.Next.Links.AccessRights.HRef, ShouldEqual, result.Next.Links.AccessRights.HRef)
				So(item.Next.Links.Editions.HRef, ShouldEqual, result.Next.Links.Editions.HRef)
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, result.Next.Links.LatestVersion.HRef)
				So(item.Next.Links.Self.HRef, ShouldEqual, result.Next.Links.Self.HRef)
				So(item.Next.Links.Taxonomy.HRef, ShouldEqual, result.Next.Links.Taxonomy.HRef)
			})
		})

		Convey("When the dataset links are nil", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
				},
				Next: &models.Dataset{
					ID: "123",
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links, ShouldBeNil)
				So(item.Next.Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
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

			item, err := RewriteDatasetWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links, ShouldResemble, &models.DatasetLinks{})
				So(item.Next.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})
	})
}

func TestRewriteDatasetWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		Convey("When the dataset links are unable to be parsed", func() {
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

			item, err := RewriteDatasetWithAuth(ctx, result, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the dataset is empty", func() {
			result := &models.DatasetUpdate{}

			item, err := RewriteDatasetWithAuth(ctx, result, linksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})

		Convey("When the dataset is nil", func() {
			item, err := RewriteDatasetWithAuth(ctx, nil, linksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		Convey("When the dataset links need rewriting", func() {
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

			item, err := RewriteDatasetWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(item.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(item.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(item.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(item.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
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

			item, err := RewriteDatasetWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should not be rewritten", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links.AccessRights.HRef, ShouldEqual, result.Current.Links.AccessRights.HRef)
				So(item.Links.Editions.HRef, ShouldEqual, result.Current.Links.Editions.HRef)
				So(item.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Links.Taxonomy.HRef, ShouldEqual, result.Current.Links.Taxonomy.HRef)
			})
		})

		Convey("When the dataset links are nil", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
				},
				Next: &models.Dataset{
					ID: "123",
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
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

			item, err := RewriteDatasetWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})
	})
}

func TestRewriteDatasetWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		Convey("When the dataset links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the dataset is empty", func() {
			result := &models.DatasetUpdate{}

			item, err := RewriteDatasetWithoutAuth(ctx, result, linksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})

		Convey("When the dataset is nil", func() {
			item, err := RewriteDatasetWithoutAuth(ctx, nil, linksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})
	})
}
