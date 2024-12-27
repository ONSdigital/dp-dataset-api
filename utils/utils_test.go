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

			Convey("Then the links should remain the same", func() {
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

			Convey("Then the links should remain the same", func() {
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

			Convey("Then the links should remain the same", func() {
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

			Convey("Then the links should remain the same", func() {
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

func TestRewriteDatasetLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dataset links", t, func() {
		Convey("When the dataset links need rewriting", func() {
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

			err := RewriteDatasetLinks(ctx, links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
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

			err := RewriteDatasetLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links are empty", func() {
			links := &models.DatasetLinks{}

			err := RewriteDatasetLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the dataset links are nil", func() {
			err := RewriteDatasetLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dataset links", t, func() {
		Convey("When the dataset links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimensions", t, func() {
		Convey("When the dimension links need rewriting", func() {
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

			items, err := RewriteDimensions(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(items[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(items[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(items[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[1].Name, ShouldEqual, "geography")
			})
		})

		Convey("When the dimension links do not need rewriting", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
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
							HRef: "http://localhost:22000/code-lists/uk-only",
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

			items, err := RewriteDimensions(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, results[0].Links.CodeList.HRef)
				So(items[0].Links.Options.HRef, ShouldEqual, results[0].Links.Options.HRef)
				So(items[0].Links.Version.HRef, ShouldEqual, results[0].Links.Version.HRef)
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, results[1].Links.CodeList.HRef)
				So(items[1].Links.Options.HRef, ShouldEqual, results[1].Links.Options.HRef)
				So(items[1].Links.Version.HRef, ShouldEqual, results[1].Links.Version.HRef)
				So(items[1].Name, ShouldEqual, "geography")
			})
		})

		Convey("When the dimension links are empty", func() {
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

			items, err := RewriteDimensions(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links, ShouldResemble, models.DimensionLink{})
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links, ShouldResemble, models.DimensionLink{})
				So(items[1].Name, ShouldEqual, "geography")
			})
		})

		Convey("When the dimensions are nil", func() {
			items, err := RewriteDimensions(ctx, nil, linksBuilder)

			Convey("Then the dimensions should remain nil", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeNil)
			})
		})

		Convey("When the dimensions are empty", func() {
			items, err := RewriteDimensions(ctx, []models.Dimension{}, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []models.Dimension{})
			})
		})
	})
}

func TestRewriteDimensions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimensions", t, func() {
		Convey("When the dimension links are unable to be parsed", func() {
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

			items, err := RewriteDimensions(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension links", t, func() {
		Convey("When the dimension links need rewriting", func() {
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

			err := RewriteDimensionLinks(ctx, &links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension links do not need rewriting", func() {
			links := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
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

			err := RewriteDimensionLinks(ctx, &links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension links are empty", func() {
			links := models.DimensionLink{}

			err := RewriteDimensionLinks(ctx, &links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, models.DimensionLink{})
			})
		})

		Convey("When the dimension links are nil", func() {
			err := RewriteDimensionLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDimensionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension links", t, func() {
		Convey("When the dimension links are unable to be parsed", func() {
			links := models.DimensionLink{
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
			}

			err := RewriteDimensionLinks(ctx, &links, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewritePublicDimensionOptions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of public dimension options", t, func() {
		Convey("When the public dimension options need rewriting", func() {
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

			items, err := RewritePublicDimensionOptions(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(items[0].Links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1A0")
				So(items[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[1].Links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/codes/K02000001")
				So(items[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[1].Name, ShouldEqual, "geography")
				So(items[2].Label, ShouldEqual, "Time")
				So(items[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(items[2].Links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/codes/Apr-00")
				So(items[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[2].Name, ShouldEqual, "time")
			})
		})

		Convey("When the public dimension options do not need rewriting", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
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
							HRef: "http://localhost:22000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/uk-only/codes/K02000001",
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
							HRef: "http://localhost:22000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/mmm-yy/codes/Apr-00",
							ID:   "time",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "time",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, results[0].Links.CodeList.HRef)
				So(items[0].Links.Code.HRef, ShouldEqual, results[0].Links.Code.HRef)
				So(items[0].Links.Version.HRef, ShouldEqual, results[0].Links.Version.HRef)
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, results[1].Links.CodeList.HRef)
				So(items[1].Links.Code.HRef, ShouldEqual, results[1].Links.Code.HRef)
				So(items[1].Links.Version.HRef, ShouldEqual, results[1].Links.Version.HRef)
				So(items[1].Name, ShouldEqual, "geography")
				So(items[2].Label, ShouldEqual, "Time")
				So(items[2].Links.CodeList.HRef, ShouldEqual, results[2].Links.CodeList.HRef)
				So(items[2].Links.Code.HRef, ShouldEqual, results[2].Links.Code.HRef)
				So(items[2].Links.Version.HRef, ShouldEqual, results[2].Links.Version.HRef)
				So(items[2].Name, ShouldEqual, "time")
			})
		})

		Convey("When the public dimension options are nil", func() {
			items, err := RewritePublicDimensionOptions(ctx, nil, linksBuilder)

			Convey("Then the public dimension options should remain nil", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeNil)
			})
		})

		Convey("When the public dimension options are empty", func() {
			items, err := RewritePublicDimensionOptions(ctx, []*models.PublicDimensionOption{}, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.PublicDimensionOption{})
			})
		})

		Convey("When the public dimension option links are empty", func() {
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

			items, err := RewritePublicDimensionOptions(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(items[1].Name, ShouldEqual, "geography")
			})
		})
	})
}

func TestRewritePublicDimensionOptions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of public dimension options", t, func() {
		Convey("When the public dimension options are unable to be parsed", func() {
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

			items, err := RewritePublicDimensionOptions(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionOptions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimension options", t, func() {
		Convey("When the dimension options need rewriting", func() {
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

			err := RewriteDimensionOptions(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(results[0].Label, ShouldEqual, "May-89")
				So(results[0].Links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/codes/May-89")
				So(results[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[0].Name, ShouldEqual, "time")
				So(results[0].NodeID, ShouldEqual, "_37abc12d_time_May-89")
				So(results[0].Option, ShouldEqual, "May-89")
				So(results[1].Label, ShouldEqual, "01.1 Food")
				So(results[1].Links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				So(results[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(results[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[1].Name, ShouldEqual, "aggregate")
				So(results[1].NodeID, ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				So(results[1].Option, ShouldEqual, "cpih1dim1G10100")
				So(results[2].Label, ShouldEqual, "Mar-02")
				So(results[2].Links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/codes/Mar-02")
				So(results[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[2].Name, ShouldEqual, "time")
				So(results[2].NodeID, ShouldEqual, "_37abc12d_time_Mar-02")
				So(results[2].Option, ShouldEqual, "Mar-02")
			})
		})

		Convey("When the dimension options do not need rewriting", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/mmm-yy",
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
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
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
							HRef: "http://localhost:22000/code-lists/mmm-yy/codes/Mar-02",
							ID:   "Mar-02",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22000/code-lists/mmm-yy",
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

			err := RewriteDimensionOptions(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(results[0].Label, ShouldEqual, "May-89")
				So(results[0].Links.Code.HRef, ShouldEqual, results[0].Links.Code.HRef)
				So(results[0].Links.CodeList.HRef, ShouldEqual, results[0].Links.CodeList.HRef)
				So(results[0].Links.Version.HRef, ShouldEqual, results[0].Links.Version.HRef)
				So(results[0].Name, ShouldEqual, "time")
				So(results[0].NodeID, ShouldEqual, "_37abc12d_time_May-89")
				So(results[0].Option, ShouldEqual, "May-89")
				So(results[1].Label, ShouldEqual, "01.1 Food")
				So(results[1].Links.Code.HRef, ShouldEqual, results[1].Links.Code.HRef)
				So(results[1].Links.CodeList.HRef, ShouldEqual, results[1].Links.CodeList.HRef)
				So(results[1].Links.Version.HRef, ShouldEqual, results[1].Links.Version.HRef)
				So(results[1].Name, ShouldEqual, "aggregate")
				So(results[1].NodeID, ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				So(results[1].Option, ShouldEqual, "cpih1dim1G10100")
				So(results[2].Label, ShouldEqual, "Mar-02")
				So(results[2].Links.Code.HRef, ShouldEqual, results[2].Links.Code.HRef)
				So(results[2].Links.CodeList.HRef, ShouldEqual, results[2].Links.CodeList.HRef)
				So(results[2].Links.Version.HRef, ShouldEqual, results[2].Links.Version.HRef)
				So(results[2].Name, ShouldEqual, "time")
				So(results[2].NodeID, ShouldEqual, "_37abc12d_time_Mar-02")
				So(results[2].Option, ShouldEqual, "Mar-02")
			})
		})

		Convey("When the dimension options are nil", func() {
			err := RewriteDimensionOptions(ctx, nil, linksBuilder)

			Convey("Then the dimension options should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When the dimension options are empty", func() {
			err := RewriteDimensionOptions(ctx, []*models.DimensionOption{}, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When the dimension option links are empty", func() {
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

			err := RewriteDimensionOptions(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(results[0].Label, ShouldEqual, "May-89")
				So(results[0].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(results[0].Name, ShouldEqual, "time")
				So(results[0].NodeID, ShouldEqual, "_37abc12d_time_May-89")
				So(results[0].Option, ShouldEqual, "May-89")
				So(results[1].Label, ShouldEqual, "01.1 Food")
				So(results[1].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(results[1].Name, ShouldEqual, "aggregate")
				So(results[1].NodeID, ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				So(results[1].Option, ShouldEqual, "cpih1dim1G10100")
			})
		})
	})
}

func TestRewriteDimensionOptions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimension options", t, func() {
		Convey("When the dimension options are unable to be parsed", func() {
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

			err := RewriteDimensionOptions(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionOptionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension option links", t, func() {
		Convey("When the dimension option links need rewriting", func() {
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

			err := RewriteDimensionOptionLinks(ctx, &links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				So(links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension option links do not need rewriting", func() {
			links := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.Code.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				So(links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension option links are empty", func() {
			links := models.DimensionOptionLinks{}

			err := RewriteDimensionOptionLinks(ctx, &links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, models.DimensionOptionLinks{})
			})
		})

		Convey("When the dimension option links are nil", func() {
			err := RewriteDimensionOptionLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDimensionOptionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension option links", t, func() {
		Convey("When the dimension option links are unable to be parsed", func() {
			links := models.DimensionOptionLinks{
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
			}

			err := RewriteDimensionOptionLinks(ctx, &links, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionsWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		Convey("When the edition update links need rewriting", func() {
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

			items, err := RewriteEditionsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Current.Edition, ShouldEqual, "time-series")
				So(items[0].Current.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Current.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So(items[0].Current.State, ShouldEqual, "edition-confirmed")
				So(items[0].Next.Edition, ShouldEqual, "time-series")
				So(items[0].Next.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Next.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So(items[0].Next.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links do not need rewriting", func() {
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

			items, err := RewriteEditionsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].Current.Edition, ShouldEqual, results[0].Current.Edition)
				So(items[0].Current.Links.Dataset.HRef, ShouldEqual, results[0].Current.Links.Dataset.HRef)
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Current.Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Current.Links.Versions.HRef, ShouldEqual, results[0].Current.Links.Versions.HRef)
				So(items[0].Current.State, ShouldEqual, results[0].Current.State)
				So(items[0].Next.Edition, ShouldEqual, results[0].Next.Edition)
				So(items[0].Next.Links.Dataset.HRef, ShouldEqual, results[0].Next.Links.Dataset.HRef)
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, results[0].Next.Links.LatestVersion.HRef)
				So(items[0].Next.Links.Self.HRef, ShouldEqual, results[0].Next.Links.Self.HRef)
				So(items[0].Next.Links.Versions.HRef, ShouldEqual, results[0].Next.Links.Versions.HRef)
				So(items[0].Next.State, ShouldEqual, results[0].Next.State)
			})
		})

		Convey("When the edition updates are nil", func() {
			items, err := RewriteEditionsWithAuth(ctx, nil, linksBuilder)

			Convey("Then the edition updates should remain nil", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeNil)
			})
		})

		Convey("When the edition updates are empty", func() {
			items, err := RewriteEditionsWithAuth(ctx, []*models.EditionUpdate{}, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.EditionUpdate{})
			})
		})

		Convey("When the edition update links are empty", func() {
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

			items, err := RewriteEditionsWithAuth(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Current.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(items[0].Current.State, ShouldEqual, "edition-confirmed")
				So(items[0].Next.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(items[0].Next.State, ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionsWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionsWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		Convey("When the edition update links need rewriting", func() {
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

			items, err := RewriteEditionsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Edition, ShouldEqual, "time-series")
				So(items[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So(items[0].State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links do not need rewriting", func() {
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

			items, err := RewriteEditionsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].Edition, ShouldEqual, results[0].Current.Edition)
				So(items[0].Links.Dataset.HRef, ShouldEqual, results[0].Current.Links.Dataset.HRef)
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Links.Versions.HRef, ShouldEqual, results[0].Current.Links.Versions.HRef)
				So(items[0].State, ShouldEqual, results[0].Current.State)
			})
		})

		Convey("When the edition updates are nil", func() {
			items, err := RewriteEditionsWithoutAuth(ctx, nil, linksBuilder)

			Convey("Then an empty list should be returned", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.Edition{})
			})
		})

		Convey("When the edition updates are empty", func() {
			items, err := RewriteEditionsWithoutAuth(ctx, []*models.EditionUpdate{}, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.Edition{})
			})
		})

		Convey("When the edition update links are empty", func() {
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

			items, err := RewriteEditionsWithoutAuth(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(items[0].State, ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionsWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}
