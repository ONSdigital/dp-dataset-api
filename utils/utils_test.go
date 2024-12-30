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

		Convey("When each dimension needs its link rewritten", func() {
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

			items, err := RewriteDimensions(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(items[1].HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[2].HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
			})
		})

		Convey("When each dimemsion doesn't need its link rewritten", func() {
			results := []models.Dimension{
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22000/code-lists/mmm-yy",
					ID:   "mmm-yy",
					Name: "time",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22000/code-lists/uk-only",
					ID:   "uk-only",
					Name: "geography",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
					Name: "aggregate",
				},
			}

			items, err := RewriteDimensions(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].HRef, ShouldEqual, results[0].HRef)
				So(items[1].HRef, ShouldEqual, results[1].HRef)
				So(items[2].HRef, ShouldEqual, results[2].HRef)
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

func TestRewriteEditionWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		Convey("When the edition update links need rewriting", func() {
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

			item, err := RewriteEditionWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Current.Edition, ShouldEqual, "time-series")
				So(item.Current.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(item.Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(item.Current.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So(item.Current.State, ShouldEqual, "edition-confirmed")
				So(item.Next.Edition, ShouldEqual, "time-series")
				So(item.Next.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2")
				So(item.Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(item.Next.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So(item.Next.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links do not need rewriting", func() {
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

			item, err := RewriteEditionWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, result.ID)
				So(item.Current.Edition, ShouldEqual, result.Current.Edition)
				So(item.Current.Links.Dataset.HRef, ShouldEqual, result.Current.Links.Dataset.HRef)
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Current.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Current.Links.Versions.HRef, ShouldEqual, result.Current.Links.Versions.HRef)
				So(item.Current.State, ShouldEqual, result.Current.State)
				So(item.Next.Edition, ShouldEqual, result.Next.Edition)
				So(item.Next.Links.Dataset.HRef, ShouldEqual, result.Next.Links.Dataset.HRef)
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, result.Next.Links.LatestVersion.HRef)
				So(item.Next.Links.Self.HRef, ShouldEqual, result.Next.Links.Self.HRef)
				So(item.Next.Links.Versions.HRef, ShouldEqual, result.Next.Links.Versions.HRef)
				So(item.Next.State, ShouldEqual, result.Next.State)
			})
		})

		Convey("When the edition update links are empty", func() {
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

			item, err := RewriteEditionWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Current.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(item.Current.State, ShouldEqual, "edition-confirmed")
				So(item.Next.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(item.Next.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links are nil", func() {
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

			item, err := RewriteEditionWithAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Current.Links, ShouldBeNil)
				So(item.Current.State, ShouldEqual, "edition-confirmed")
				So(item.Next.Links, ShouldBeNil)
				So(item.Next.State, ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
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

			item, err := RewriteEditionWithAuth(ctx, result, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the edition update is nil", func() {
			item, err := RewriteEditionWithAuth(ctx, nil, linksBuilder)

			Convey("Then an edition not found error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "edition not found")
			})
		})
	})
}

func TestRewriteEditionWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		Convey("When the edition update links need rewriting", func() {
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

			item, err := RewriteEditionWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Edition, ShouldEqual, "time-series")
				So(item.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(item.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(item.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(item.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So(item.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links do not need rewriting", func() {
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

			item, err := RewriteEditionWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, result.ID)
				So(item.Edition, ShouldEqual, result.Current.Edition)
				So(item.Links.Dataset.HRef, ShouldEqual, result.Current.Links.Dataset.HRef)
				So(item.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Links.Versions.HRef, ShouldEqual, result.Current.Links.Versions.HRef)
				So(item.State, ShouldEqual, result.Current.State)
			})
		})

		Convey("When the edition update links are empty", func() {
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

			item, err := RewriteEditionWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(item.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links are nil", func() {
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

			item, err := RewriteEditionWithoutAuth(ctx, result, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Links, ShouldBeNil)
				So(item.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update is empty", func() {
			item, err := RewriteEditionWithoutAuth(ctx, &models.EditionUpdate{}, linksBuilder)

			Convey("Then nothing should be returned", func() {
				So(err, ShouldBeNil)
				So(item, ShouldBeNil)
			})
		})

		Convey("When the edition update 'current' is nil", func() {
			item, err := RewriteEditionWithoutAuth(ctx, &models.EditionUpdate{
				Current: nil,
				Next:    &models.Edition{},
			}, linksBuilder)

			Convey("Then nothing should be returned", func() {
				So(err, ShouldBeNil)
				So(item, ShouldBeNil)
			})
		})
	})
}

func TestRewriteEditionWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the edition update is nil", func() {
			item, err := RewriteEditionWithoutAuth(ctx, nil, linksBuilder)

			Convey("Then an edition not found error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "edition not found")
			})
		})
	})
}

func TestRewriteEditionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of edition update links", t, func() {
		Convey("When the edition update links need rewriting", func() {
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

			err := RewriteEditionLinks(ctx, links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
			})
		})

		Convey("When the edition update links do not need rewriting", func() {
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

			err := RewriteEditionLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
			})
		})

		Convey("When the edition update links are empty", func() {
			links := &models.EditionUpdateLinks{}

			err := RewriteEditionLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, &models.EditionUpdateLinks{})
			})
		})

		Convey("When the edition update links are nil", func() {
			err := RewriteEditionLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteEditionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of edition update links", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteMetadataLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of metadata links", t, func() {
		Convey("When the metadata links need rewriting", func() {
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

			err := RewriteMetadataLinks(ctx, links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Version.ID, ShouldEqual, "1")
				So(links.WebsiteVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the metadata links do not need rewriting", func() {
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
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.AccessRights.HRef, ShouldEqual, "http://localhost:22000/accessrights")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Version.ID, ShouldEqual, "1")
				So(links.WebsiteVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the metadata links are empty", func() {
			links := &models.MetadataLinks{}

			err := RewriteMetadataLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, &models.MetadataLinks{})
			})
		})

		Convey("When the metadata links are nil", func() {
			err := RewriteMetadataLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When the metadata links are missing", func() {
			err := RewriteMetadataLinks(ctx, &models.MetadataLinks{}, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteMetadataLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of metadata links", t, func() {
		Convey("When the metadata links are unable to be parsed", func() {
			links := &models.MetadataLinks{
				AccessRights: &models.LinkObject{
					HRef: "://oldhost:1000/accessrights",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, links, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteVersions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of versions", t, func() {
		Convey("When the version and dimension links need rewriting", func() {
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
							HRef:  "https://oldhost:1000/code-lists/mmm-yy",
							ID:    "mmm-yy",
							Label: "Time",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy/versions/1",
								},
							},
							Name: "time",
						},
						{
							HRef:  "https://oldhost:1000/code-lists/uk-only",
							ID:    "uk-only",
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only/versions/1",
								},
							},
							Name: "geography",
						},
						{
							HRef:  "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/versions/1",
								},
							},
							Name: "aggregate",
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
							HRef:  "https://oldhost:1000/code-lists/mmm-yy",
							ID:    "mmm-yy",
							Label: "Time",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy/versions/1",
								},
							},
							Name: "time",
						},
						{
							HRef:  "https://oldhost:1000/code-lists/uk-only",
							ID:    "uk-only",
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only/versions/1",
								},
							},
							Name: "geography",
						},
						{
							HRef:  "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/versions/1",
								},
							},
							Name: "aggregate",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, "cf4b2196-3548-4bd5-8288-92fe4ca06327")
				So(items[0].DatasetID, ShouldEqual, "cpih01")
				So(items[0].Edition, ShouldEqual, "time-series")
				So(items[0].Version, ShouldEqual, 53)
				So(items[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/53")
				So(items[0].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(items[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(items[0].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/options")
				So(items[0].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/versions/1")
				So(items[0].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[0].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/options")
				So(items[0].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/versions/1")
				So(items[0].Dimensions[2].HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(items[0].Dimensions[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(items[0].Dimensions[2].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/options")
				So(items[0].Dimensions[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1")

				So(items[1].ID, ShouldEqual, "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b")
				So(items[1].DatasetID, ShouldEqual, "cpih01")
				So(items[1].Edition, ShouldEqual, "time-series")
				So(items[1].Version, ShouldEqual, 52)
				So(items[1].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[1].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[1].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/52")
				So(items[1].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(items[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(items[1].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/options")
				So(items[1].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/versions/1")
				So(items[1].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(items[1].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/options")
				So(items[1].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/versions/1")
				So(items[1].Dimensions[2].HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(items[1].Dimensions[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(items[1].Dimensions[2].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/options")
				So(items[1].Dimensions[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1")
			})
		})

		Convey("When the version and dimension links do not need rewriting", func() {
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
							HRef:  "http://localhost:22000/code-lists/mmm-yy",
							ID:    "mmm-yy",
							Label: "Time",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy/versions/1",
								},
							},
							Name: "time",
						},
						{
							HRef:  "http://localhost:22000/code-lists/uk-only",
							ID:    "uk-only",
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only/versions/1",
								},
							},
							Name: "geography",
						},
						{
							HRef:  "http://localhost:22000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1",
								},
							},
							Name: "aggregate",
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
							HRef:  "http://localhost:22000/code-lists/mmm-yy",
							ID:    "mmm-yy",
							Label: "Time",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy/versions/1",
								},
							},
							Name: "time",
						},
						{
							HRef:  "http://localhost:22000/code-lists/uk-only",
							ID:    "uk-only",
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only/versions/1",
								},
							},
							Name: "geography",
						},
						{
							HRef:  "http://localhost:22000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1",
								},
							},
							Name: "aggregate",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].DatasetID, ShouldEqual, results[0].DatasetID)
				So(items[0].Edition, ShouldEqual, results[0].Edition)
				So(items[0].Version, ShouldEqual, results[0].Version)
				So(items[0].Links.Dataset.HRef, ShouldEqual, results[0].Links.Dataset.HRef)
				So(items[0].Links.Edition.HRef, ShouldEqual, results[0].Links.Edition.HRef)
				So(items[0].Links.Self.HRef, ShouldEqual, results[0].Links.Self.HRef)
				So(items[0].Dimensions[0].HRef, ShouldEqual, results[0].Dimensions[0].HRef)
				So(items[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, results[0].Dimensions[0].Links.CodeList.HRef)
				So(items[0].Dimensions[0].Links.Options.HRef, ShouldEqual, results[0].Dimensions[0].Links.Options.HRef)
				So(items[0].Dimensions[0].Links.Version.HRef, ShouldEqual, results[0].Dimensions[0].Links.Version.HRef)
				So(items[0].Dimensions[1].HRef, ShouldEqual, results[0].Dimensions[1].HRef)
				So(items[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, results[0].Dimensions[1].Links.CodeList.HRef)
				So(items[0].Dimensions[1].Links.Options.HRef, ShouldEqual, results[0].Dimensions[1].Links.Options.HRef)
				So(items[0].Dimensions[1].Links.Version.HRef, ShouldEqual, results[0].Dimensions[1].Links.Version.HRef)
				So(items[0].Dimensions[2].HRef, ShouldEqual, results[0].Dimensions[2].HRef)
				So(items[0].Dimensions[2].Links.CodeList.HRef, ShouldEqual, results[0].Dimensions[2].Links.CodeList.HRef)
				So(items[0].Dimensions[2].Links.Options.HRef, ShouldEqual, results[0].Dimensions[2].Links.Options.HRef)
				So(items[0].Dimensions[2].Links.Version.HRef, ShouldEqual, results[0].Dimensions[2].Links.Version.HRef)

				So(items[1].ID, ShouldEqual, results[1].ID)
				So(items[1].DatasetID, ShouldEqual, results[1].DatasetID)
				So(items[1].Edition, ShouldEqual, results[1].Edition)
				So(items[1].Version, ShouldEqual, results[1].Version)
				So(items[1].Links.Dataset.HRef, ShouldEqual, results[1].Links.Dataset.HRef)
				So(items[1].Links.Edition.HRef, ShouldEqual, results[1].Links.Edition.HRef)
				So(items[1].Links.Self.HRef, ShouldEqual, results[1].Links.Self.HRef)
				So(items[1].Dimensions[0].HRef, ShouldEqual, results[1].Dimensions[0].HRef)
				So(items[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, results[1].Dimensions[0].Links.CodeList.HRef)
				So(items[1].Dimensions[0].Links.Options.HRef, ShouldEqual, results[1].Dimensions[0].Links.Options.HRef)
				So(items[1].Dimensions[0].Links.Version.HRef, ShouldEqual, results[1].Dimensions[0].Links.Version.HRef)
				So(items[1].Dimensions[1].HRef, ShouldEqual, results[1].Dimensions[1].HRef)
				So(items[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, results[1].Dimensions[1].Links.CodeList.HRef)
				So(items[1].Dimensions[1].Links.Options.HRef, ShouldEqual, results[1].Dimensions[1].Links.Options.HRef)
				So(items[1].Dimensions[1].Links.Version.HRef, ShouldEqual, results[1].Dimensions[1].Links.Version.HRef)
				So(items[1].Dimensions[2].HRef, ShouldEqual, results[1].Dimensions[2].HRef)
				So(items[1].Dimensions[2].Links.CodeList.HRef, ShouldEqual, results[1].Dimensions[2].Links.CodeList.HRef)
				So(items[1].Dimensions[2].Links.Options.HRef, ShouldEqual, results[1].Dimensions[2].Links.Options.HRef)
				So(items[1].Dimensions[2].Links.Version.HRef, ShouldEqual, results[1].Dimensions[2].Links.Version.HRef)
			})
		})

		Convey("When the version and dimension links are empty", func() {
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

			items, err := RewriteVersions(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].DatasetID, ShouldEqual, results[0].DatasetID)
				So(items[0].Edition, ShouldEqual, results[0].Edition)
				So(items[0].Version, ShouldEqual, results[0].Version)
				So(items[0].Links, ShouldResemble, &models.VersionLinks{})
				So(items[0].Dimensions, ShouldResemble, []models.Dimension{})

				So(items[1].ID, ShouldEqual, results[1].ID)
				So(items[1].DatasetID, ShouldEqual, results[1].DatasetID)
				So(items[1].Edition, ShouldEqual, results[1].Edition)
				So(items[1].Version, ShouldEqual, results[1].Version)
				So(items[1].Links, ShouldResemble, &models.VersionLinks{})
				So(items[1].Dimensions, ShouldResemble, []models.Dimension{})
			})
		})

		Convey("When the version and dimension links are nil", func() {
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

			items, err := RewriteVersions(ctx, results, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].DatasetID, ShouldEqual, results[0].DatasetID)
				So(items[0].Edition, ShouldEqual, results[0].Edition)
				So(items[0].Version, ShouldEqual, results[0].Version)
				So(items[0].Links, ShouldBeNil)
				So(items[0].Dimensions, ShouldBeNil)

				So(items[1].ID, ShouldEqual, results[1].ID)
				So(items[1].DatasetID, ShouldEqual, results[1].DatasetID)
				So(items[1].Edition, ShouldEqual, results[1].Edition)
				So(items[1].Version, ShouldEqual, results[1].Version)
				So(items[1].Links, ShouldBeNil)
				So(items[1].Dimensions, ShouldBeNil)
			})
		})
	})
}

func TestRewriteVersions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of versions", t, func() {
		Convey("When the version and dimension links are unable to be parsed", func() {
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
					Dimensions: []models.Dimension{
						{
							HRef:  "://oldhost:1000/code-lists/mmm-yy",
							ID:    "mmm-yy",
							Label: "Time",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							Name: "time",
						},
						{
							HRef:  "://oldhost:1000/code-lists/uk-only",
							ID:    "uk-only",
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							Name: "geography",
						},
						{
							HRef:  "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							Name: "aggregate",
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
					Dimensions: []models.Dimension{
						{
							HRef:  "://oldhost:1000/code-lists/mmm-yy",
							ID:    "mmm-yy",
							Label: "Time",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							Name: "time",
						},
						{
							HRef:  "://oldhost:1000/code-lists/uk-only",
							ID:    "uk-only",
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							Name: "geography",
						},
						{
							HRef:  "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							Name: "aggregate",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteVersionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of version links", t, func() {
		Convey("When the version links need rewriting", func() {
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

			err := RewriteVersionLinks(ctx, links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Spatial.HRef, ShouldEqual, "https://oldhost:1000/spatial")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Version.ID, ShouldEqual, "1")
			})
		})

		Convey("When the version links do not need rewriting", func() {
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

			err := RewriteVersionLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(links.Version.ID, ShouldEqual, "1")
			})
		})

		Convey("When the version links are empty", func() {
			links := &models.VersionLinks{}

			err := RewriteVersionLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, &models.VersionLinks{})
			})
		})

		Convey("When the version links are nil", func() {
			err := RewriteVersionLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteVersionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of version links", t, func() {
		Convey("When the version links are unable to be parsed", func() {
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

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteInstances_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of instances", t, func() {
		Convey("When the instance links need rewriting", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "City",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/city",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/city/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/city/versions/1",
								},
							},
							HRef: "https://oldhost:1000/city",
							ID:   "city",
						},
						{
							Label: "Number of siblings (3 mappings)",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/siblings_3",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/siblings_3/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/siblings_3/versions/1",
								},
							},
							HRef: "https://oldhost:1000/siblings_3",
							ID:   "siblings_3",
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
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
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
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/mmm-yy/versions/1",
								},
							},
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
							Name: "time",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only/versions/1",
								},
							},
							HRef: "https://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
							Name: "geography",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/options",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/versions/1",
								},
							},
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
							Name: "aggregate",
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
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
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
						Version: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)

				So(results[0].CollectionID, ShouldEqual, "cantabularflexibledefault-1")
				So(results[0].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/city")
				So(results[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/city")
				So(results[0].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/city/options")
				So(results[0].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/city/versions/1")
				So(results[0].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/siblings_3")
				So(results[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/siblings_3")
				So(results[0].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/siblings_3/options")
				So(results[0].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/siblings_3/versions/1")
				So(results[0].Edition, ShouldEqual, "2021")
				So(results[0].InstanceID, ShouldEqual, "1")
				So(results[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default")
				So(results[0].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[0].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021")
				So(results[0].Links.Job.HRef, ShouldEqual, "http://localhost:22000/jobs/1")
				So(results[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(results[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1")

				So(results[1].CollectionID, ShouldEqual, "cpihtest-1")
				So(results[1].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/options")
				So(results[1].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/versions/1")
				So(results[1].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(results[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(results[1].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/options")
				So(results[1].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/versions/1")
				So(results[1].Dimensions[2].HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(results[1].Dimensions[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(results[1].Dimensions[2].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/options")
				So(results[1].Dimensions[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1")
				So(results[1].Edition, ShouldEqual, "time-series")
				So(results[1].InstanceID, ShouldEqual, "2")
				So(results[1].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(results[1].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(results[1].Links.Job.HRef, ShouldEqual, "http://localhost:22000/jobs/2")
				So(results[1].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/2")
				So(results[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance links do not need rewriting", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "City",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/city",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/city/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/city/versions/1",
								},
							},
							HRef: "http://localhost:22000/city",
							ID:   "city",
						},
						{
							Label: "Number of siblings (3 mappings)",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/siblings_3",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/siblings_3/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/siblings_3/versions/1",
								},
							},
							HRef: "http://localhost:22000/siblings_3",
							ID:   "siblings_3",
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
							HRef: "http://localhost:22000/code-lists/mmm-yy",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "http://localhost:22000/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/1",
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
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/mmm-yy/versions/1",
								},
							},
							HRef: "http://localhost:22000/code-lists/mmm-yy",
							ID:   "mmm-yy",
							Name: "time",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/uk-only/versions/1",
								},
							},
							HRef: "http://localhost:22000/code-lists/uk-only",
							ID:   "uk-only",
							Name: "geography",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/options",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1",
								},
							},
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
							Name: "aggregate",
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
							HRef: "http://localhost:22000/code-lists/mmm-yy",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "http://localhost:22000/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/2",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)

				So(results[0].CollectionID, ShouldEqual, "cantabularflexibledefault-1")
				So(results[0].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/city")
				So(results[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/city")
				So(results[0].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/city/options")
				So(results[0].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/city/versions/1")
				So(results[0].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/siblings_3")
				So(results[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/siblings_3")
				So(results[0].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/siblings_3/options")
				So(results[0].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/siblings_3/versions/1")
				So(results[0].Edition, ShouldEqual, "2021")
				So(results[0].InstanceID, ShouldEqual, "1")
				So(results[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default")
				So(results[0].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[0].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021")
				So(results[0].Links.Job.HRef, ShouldEqual, "http://localhost:22000/jobs/1")
				So(results[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(results[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1")

				So(results[1].CollectionID, ShouldEqual, "cpihtest-1")
				So(results[1].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/options")
				So(results[1].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy/versions/1")
				So(results[1].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(results[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(results[1].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/options")
				So(results[1].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only/versions/1")
				So(results[1].Dimensions[2].HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(results[1].Dimensions[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(results[1].Dimensions[2].Links.Options.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/options")
				So(results[1].Dimensions[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid/versions/1")
				So(results[1].Edition, ShouldEqual, "time-series")
				So(results[1].InstanceID, ShouldEqual, "2")
				So(results[1].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(results[1].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(results[1].Links.Job.HRef, ShouldEqual, "http://localhost:22000/jobs/2")
				So(results[1].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/2")
				So(results[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance links are empty", func() {
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
							HRef: "http://localhost:22000/city",
							ID:   "city",
						},
						{
							Label: "Number of siblings (3 mappings)",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "http://localhost:22000/siblings_3",
							ID:   "siblings_3",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
					Links:      &models.InstanceLinks{},
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
							HRef: "http://localhost:22000/code-lists/mmm-yy",
							ID:   "mmm-yy",
							Name: "time",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "http://localhost:22000/code-lists/uk-only",
							ID:   "uk-only",
							Name: "geography",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "http://localhost:22000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
							Name: "aggregate",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
					Links:      &models.InstanceLinks{},
				},
			}

			err := RewriteInstances(ctx, results, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)

				So(results[0].CollectionID, ShouldEqual, "cantabularflexibledefault-1")
				So(results[0].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/city")
				So(results[0].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/siblings_3")
				So(results[0].Edition, ShouldEqual, "2021")
				So(results[0].InstanceID, ShouldEqual, "1")
				So(results[0].Links, ShouldResemble, &models.InstanceLinks{})

				So(results[1].CollectionID, ShouldEqual, "cpihtest-1")
				So(results[1].Dimensions[0].HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(results[1].Dimensions[1].HRef, ShouldEqual, "http://localhost:22000/code-lists/uk-only")
				So(results[1].Dimensions[2].HRef, ShouldEqual, "http://localhost:22000/code-lists/cpih1dim1aggid")
				So(results[1].Edition, ShouldEqual, "time-series")
				So(results[1].InstanceID, ShouldEqual, "2")
				So(results[1].Links, ShouldResemble, &models.InstanceLinks{})
			})
		})
	})
}

func TestRewriteInstances_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of instances", t, func() {
		Convey("When the instance links are unable to be parsed", func() {
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
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "://oldhost:1000/code-lists/mmm-yy",
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
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "://oldhost:1000/code-lists/mmm-yy",
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

			err := RewriteInstances(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteInstanceLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of instance links", t, func() {
		Convey("When the instance links need rewriting", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/mmm-yy",
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
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, linksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(links.Job.HRef, ShouldEqual, "http://localhost:22000/jobs/1")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance links do not need rewriting", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/code-lists/mmm-yy",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "http://localhost:22000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/1",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/code-lists/mmm-yy")
				So(links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(links.Job.HRef, ShouldEqual, "http://localhost:22000/jobs/1")
				So(links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance links are empty", func() {
			links := &models.InstanceLinks{}

			err := RewriteInstanceLinks(ctx, links, linksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(links, ShouldResemble, &models.InstanceLinks{})
			})
		})

		Convey("When the instance links are nil", func() {
			err := RewriteInstanceLinks(ctx, nil, linksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteInstanceLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of instance links", t, func() {
		Convey("When the instance links are unable to be parsed", func() {
			links := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "://oldhost:1000/code-lists/mmm-yy",
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
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, links, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}
