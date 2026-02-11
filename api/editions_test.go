package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"time"

	"github.com/ONSdigital/dp-authorisation/v2/authorisation"
	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	applicationMocks "github.com/ONSdigital/dp-dataset-api/application/mock"
	cloudflareMocks "github.com/ONSdigital/dp-dataset-api/cloudflare/mocks"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/utils"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

var exampleStaticVersion = &models.Version{
	DatasetID:   "123-456",
	Edition:     "678",
	ReleaseDate: "1996-04-01",
	Version:     1,
	Links: &models.VersionLinks{
		Dataset: &models.LinkObject{
			HRef: "http://localhost:22000/datasets/123-456",
			ID:   "123-456",
		},
		Self: &models.LinkObject{
			HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
		},
		Edition: &models.LinkObject{
			HRef: "http://localhost:22000/datasets/123-456/editions/678",
			ID:   "678",
		},
		Version: &models.LinkObject{
			HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
			ID:   "1",
		},
	},
	LastUpdated: time.Date(2025, 3, 11, 0, 0, 0, 0, time.UTC),
	State:       models.AssociatedState,
}

func TestGetEditionsForbidden(t *testing.T) {
	t.Parallel()
	Convey("When a request to get editions is forbidden 403 returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		Convey("When Dataset Type is static", func() {
			mockedDataStore := &storetest.StorerMock{
				IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
					return false, nil
				},
			}
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusForbidden)
			So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 0)
		})
		Convey("When Dataset Type is not static", func() {
			mockedDataStore := &storetest.StorerMock{
				IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
					return true, nil
				},
			}
			api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
			api.Router.ServeHTTP(w, r)

			So(w.Code, ShouldEqual, http.StatusForbidden)
			So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetEditionsUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("When a request to get editions is unauthorised, then no expected database calls are made", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})
}

func TestGetEditionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("get editions delegates offset and limit to db func and returns results list", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456"})

		publicResult := &models.Edition{ID: "20"}
		results := []*models.EditionUpdate{{Current: publicResult}}

		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return results, 2, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		list, totalCount, err := api.getEditions(w, r, 20, 0)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(list, ShouldResemble, []*models.Edition{publicResult})
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})

	Convey("get published editions when the dataset type is static", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456"})
		w := httptest.NewRecorder()

		editionsList := []*models.EditionUpdate{
			{
				Current: &models.Edition{
					Edition:     "2023",
					ID:          "",
					DatasetID:   "123",
					Version:     2,
					LastUpdated: time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
					ReleaseDate: "2023-10-01",
					Links: &models.EditionUpdateLinks{
						Dataset:       &models.LinkObject{HRef: "http://localhost:22000/datasets/123", ID: "123"},
						LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2", ID: "2"},
						Self:          &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023", ID: "2023"},
						Versions:      &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions", ID: ""},
					},
					State: models.PublishedState,
					Alerts: &[]models.Alert{
						{
							Date:        "",
							Description: "Test alert",
							Type:        models.AlertType(""),
						},
					},
					UsageNotes: &[]models.UsageNote{
						{
							Note:  "Test usage note",
							Title: "",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Test distribution",
							Format:      models.DistributionFormat(""),
							MediaType:   models.DistributionMediaType(""),
							DownloadURL: "",
							ByteSize:    0,
						},
					},
					QualityDesignation: models.QualityDesignation("Test quality designation"),
				},
				Next: &models.Edition{
					Edition:     "2023",
					ID:          "",
					DatasetID:   "123",
					Version:     2,
					LastUpdated: time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
					ReleaseDate: "2023-10-01",
					Links: &models.EditionUpdateLinks{
						Dataset:       &models.LinkObject{HRef: "http://localhost:22000/datasets/123", ID: "123"},
						LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2", ID: "2"},
						Self:          &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023", ID: "2023"},
						Versions:      &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions", ID: ""},
					},
					State: models.PublishedState,
					Alerts: &[]models.Alert{
						{
							Date:        "",
							Description: "Test alert",
							Type:        models.AlertType(""),
						},
					},
					UsageNotes: &[]models.UsageNote{
						{
							Note:  "Test usage note",
							Title: "",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Test distribution",
							Format:      models.DistributionFormat(""),
							MediaType:   models.DistributionMediaType(""),
							DownloadURL: "",
							ByteSize:    0,
						},
					},
					QualityDesignation: models.QualityDesignation("Test quality designation"),
				},
			},
		}

		versions := []*models.Version{
			{
				DatasetID:   "123",
				Edition:     "2023",
				ReleaseDate: "2023-10-01",
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123",
						ID:   "123",
					},
					Version: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023/versions/1",
						ID:   "1",
					},
					Edition: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023",
						ID:   "2023",
					},
				},
				Version:            1,
				LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
				Alerts:             &[]models.Alert{{Description: "Test alert"}},
				UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
				Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
				QualityDesignation: "Test quality designation",
				State:              "published",
			},
			{
				DatasetID:   "123",
				Edition:     "2023",
				ReleaseDate: "2023-10-01",
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123",
						ID:   "123",
					},
					Version: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2",
						ID:   "2",
					},
					Edition: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023",
						ID:   "2023",
					},
				},
				Version:            2,
				LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
				Alerts:             &[]models.Alert{{Description: "Test alert"}},
				UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
				Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
				QualityDesignation: "Test quality designation",
				State:              "published",
			},
		}
		publishedLatestVersion := &models.Version{
			DatasetID:   "123",
			Edition:     "2023",
			ReleaseDate: "2023-10-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2",
					ID:   "2",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023",
					ID:   "2023",
				},
			},
			Version:            2,
			LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
			Alerts:             &[]models.Alert{{Description: "Test alert"}},
			UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
			Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
			QualityDesignation: "Test quality designation",
			State:              "published",
		}
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return models.Static.String(), nil
			},
			GetAllStaticVersionsFunc: func(ctx context.Context, ID, state string, offset, limit int) ([]*models.Version, int, error) {
				return versions, 2, nil
			},
			GetLatestVersionStaticFunc: func(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
				return publishedLatestVersion, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, totalCount, _ := api.getEditions(w, r, 20, 0)

		editions, err := utils.MapVersionsToEditionUpdate(publishedLatestVersion, nil)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetAllStaticVersionsCalls()), ShouldEqual, 1)
		So([]*models.EditionUpdate{editions}, ShouldEqual, editionsList)
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})

	Convey("get all editions when the dataset type is static", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r = mux.SetURLVars(r, map[string]string{"dataset_id": "123-456"})
		w := httptest.NewRecorder()

		editionsList := []*models.EditionUpdate{
			{
				Current: &models.Edition{
					Edition:     "2023",
					ID:          "",
					DatasetID:   "123",
					Version:     1,
					LastUpdated: time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
					ReleaseDate: "2023-10-01",
					Links: &models.EditionUpdateLinks{
						Dataset:       &models.LinkObject{HRef: "http://localhost:22000/datasets/123", ID: "123"},
						LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions/1", ID: "1"},
						Self:          &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023", ID: "2023"},
						Versions:      &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions", ID: ""},
					},
					State: models.PublishedState,
					Alerts: &[]models.Alert{
						{
							Date:        "",
							Description: "Test alert",
							Type:        models.AlertType(""),
						},
					},
					UsageNotes: &[]models.UsageNote{
						{
							Note:  "Test usage note",
							Title: "",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Test distribution",
							Format:      models.DistributionFormat(""),
							MediaType:   models.DistributionMediaType(""),
							DownloadURL: "",
							ByteSize:    0,
						},
					},
					QualityDesignation: models.QualityDesignation("Test quality designation"),
				},
				Next: &models.Edition{
					Edition:     "2023",
					ID:          "",
					DatasetID:   "123",
					Version:     2,
					LastUpdated: time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
					ReleaseDate: "2023-10-01",
					Links: &models.EditionUpdateLinks{
						Dataset:       &models.LinkObject{HRef: "http://localhost:22000/datasets/123", ID: "123"},
						LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2", ID: "2"},
						Self:          &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023", ID: "2023"},
						Versions:      &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2023/versions", ID: ""},
					},
					State: models.AssociatedState,
					Alerts: &[]models.Alert{
						{
							Date:        "",
							Description: "Test alert",
							Type:        models.AlertType(""),
						},
					},
					UsageNotes: &[]models.UsageNote{
						{
							Note:  "Test usage note",
							Title: "",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Test distribution",
							Format:      models.DistributionFormat(""),
							MediaType:   models.DistributionMediaType(""),
							DownloadURL: "",
							ByteSize:    0,
						},
					},
					QualityDesignation: models.QualityDesignation("Test quality designation"),
				},
			},
		}

		versions := []*models.Version{
			{
				DatasetID:   "123",
				Edition:     "2023",
				ReleaseDate: "2023-10-01",
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123",
						ID:   "123",
					},
					Version: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023/versions/1",
						ID:   "1",
					},
					Edition: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023",
						ID:   "2023",
					},
				},
				Version:            1,
				LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
				Alerts:             &[]models.Alert{{Description: "Test alert"}},
				UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
				Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
				QualityDesignation: "Test quality designation",
				State:              "published",
			},
			{
				DatasetID:   "123",
				Edition:     "2023",
				ReleaseDate: "2023-10-01",
				Links: &models.VersionLinks{
					Dataset: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123",
						ID:   "123",
					},
					Version: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2",
						ID:   "2",
					},
					Edition: &models.LinkObject{
						HRef: "http://localhost:22000/datasets/123/editions/2023",
						ID:   "2023",
					},
				},
				Version:            2,
				LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
				Alerts:             &[]models.Alert{{Description: "Test alert"}},
				UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
				Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
				QualityDesignation: "Test quality designation",
				State:              "associated",
			},
		}
		publishedLatestVersion := &models.Version{
			DatasetID:   "123",
			Edition:     "2023",
			ReleaseDate: "2023-10-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023/versions/1",
					ID:   "1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023",
					ID:   "2023",
				},
			},
			Version:            1,
			LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
			Alerts:             &[]models.Alert{{Description: "Test alert"}},
			UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
			Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
			QualityDesignation: "Test quality designation",
			State:              "published",
		}
		unpublishedLatestVersion := &models.Version{
			DatasetID:   "123",
			Edition:     "2023",
			ReleaseDate: "2023-10-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2",
					ID:   "2",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023",
					ID:   "2023",
				},
			},
			Version:            2,
			LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
			Alerts:             &[]models.Alert{{Description: "Test alert"}},
			UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
			Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
			QualityDesignation: "Test quality designation",
			State:              "associated",
		}
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return models.Static.String(), nil
			},
			GetAllStaticVersionsFunc: func(ctx context.Context, ID, state string, offset, limit int) ([]*models.Version, int, error) {
				return versions, 2, nil
			},
			GetLatestVersionStaticFunc: func(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
				return publishedLatestVersion, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		_, totalCount, _ := api.getEditions(w, r, 20, 0)

		editions, err := utils.MapVersionsToEditionUpdate(publishedLatestVersion, unpublishedLatestVersion)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetAllStaticVersionsCalls()), ShouldEqual, 1)
		So([]*models.EditionUpdate{editions}, ShouldEqual, editionsList)
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})
}

func TestGetEditionsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, errs.ErrInternalServer
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return "", nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, errs.ErrDatasetNotFound
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return "", nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return nil, 0, errs.ErrEditionNotFound
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return "", nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetEditionsFunc: func(context.Context, string, string, int, int, bool) ([]*models.EditionUpdate, int, error) {
				return nil, 0, errs.ErrEditionNotFound
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return "", nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no editions exist against a dataset return version not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return models.Static.String(), nil
			},
			GetAllStaticVersionsFunc: func(ctx context.Context, ID, state string, offset, limit int) ([]*models.Version, int, error) {
				return nil, 0, errs.ErrVersionsNotFound
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionsNotFound.Error())
		So(len(mockedDataStore.GetAllStaticVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionForbidden(t *testing.T) {
	t.Parallel()
	Convey("A request to get edition is forbidden, then no expected database calls are made", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})
}

func TestGetEditionUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("A request to get edition is not authorised, then no expected database calls are made", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					Current: &models.Edition{
						Edition: "678",
						State:   models.PublishedState,
					},
				}, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordEditionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, edition *models.Edition) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("A successful request to get edition when dataset is static returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return exampleStaticVersion, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-user"}, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, versionDoc *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, errs.ErrInternalServer
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return "", errs.ErrInternalServer
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, errs.ErrDatasetNotFound
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return "", errs.ErrDatasetNotFound
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("When edition does not exist for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("When edition is not published for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return false, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("When dataset is static and version does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, &applicationMocks.AuditServiceMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionRecordsAuditEvent(t *testing.T) {
	t.Parallel()

	Convey("Given an authorised request to get an edition for a static dataset", t, func() {
		publishedVersion := &models.Version{
			DatasetID:   "123-456",
			Edition:     "678",
			Version:     1,
			State:       models.PublishedState,
			ReleaseDate: "1996-04-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456",
					ID:   "123-456",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678",
					ID:   "678",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
					ID:   "1",
				},
			},
			LastUpdated: time.Date(2025, 3, 11, 0, 0, 0, 0, time.UTC),
		}

		unpublishedVersion := &models.Version{
			DatasetID:   "123-456",
			Edition:     "678",
			Version:     2,
			State:       models.AssociatedState,
			ReleaseDate: "1996-04-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456",
					ID:   "123-456",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/2",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678",
					ID:   "678",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/2",
					ID:   "2",
				},
			},
			LastUpdated: time.Date(2025, 3, 11, 0, 0, 0, 0, time.UTC),
		}

		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
				if state == models.PublishedState {
					return publishedVersion, nil
				}
				return unpublishedVersion, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, versionDoc *models.Version) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-user-id"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		Convey("When we call the GET edition endpoint", func() {
			api.Router.ServeHTTP(w, r)

			Convey("Then it returns a 200 OK", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("And the audit service is called with the correct parameters", func() {
				So(len(auditServiceMock.RecordVersionAuditEventCalls()), ShouldEqual, 1)

				call := auditServiceMock.RecordVersionAuditEventCalls()[0]
				So(call.RequestedBy.ID, ShouldEqual, "test-user-id")
				So(call.RequestedBy.Email, ShouldEqual, "test-user-id")
				So(call.Action, ShouldEqual, models.ActionRead)
				So(call.Resource, ShouldEqual, "/datasets/123-456/editions/678")
				So(call.Version, ShouldEqual, unpublishedVersion)
			})

			Convey("And the relevant calls have been made", func() {
				So(len(mockedDataStore.IsStaticDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 2)
			})
		})
	})
}

func TestGetEditionRecordsAuditEventWithPublishedVersionOnly(t *testing.T) {
	t.Parallel()

	Convey("Given an authorised request where only published version exists", t, func() {
		publishedVersion := &models.Version{
			DatasetID:   "123-456",
			Edition:     "678",
			Version:     1,
			State:       models.PublishedState,
			ReleaseDate: "1996-04-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456",
					ID:   "123-456",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678",
					ID:   "678",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
					ID:   "1",
				},
			},
			LastUpdated: time.Date(2025, 3, 11, 0, 0, 0, 0, time.UTC),
		}

		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
				if state == models.PublishedState {
					return publishedVersion, nil
				}
				return nil, errs.ErrVersionNotFound
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, versionDoc *models.Version) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-user-id"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		Convey("When we call the GET edition endpoint", func() {
			api.Router.ServeHTTP(w, r)

			Convey("Then it returns a 200 OK", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("And the audit service is called with the published version", func() {
				So(len(auditServiceMock.RecordVersionAuditEventCalls()), ShouldEqual, 1)

				call := auditServiceMock.RecordVersionAuditEventCalls()[0]
				So(call.RequestedBy.ID, ShouldEqual, "test-user-id")
				So(call.RequestedBy.Email, ShouldEqual, "test-user-id")
				So(call.Action, ShouldEqual, models.ActionRead)
				So(call.Resource, ShouldEqual, "/datasets/123-456/editions/678")
				So(call.Version, ShouldEqual, publishedVersion)
			})
		})
	})
}

func TestGetEditionDoesNotRecordAuditEventForUnauthorisedUser(t *testing.T) {
	t.Parallel()

	Convey("Given an unauthorised request to get an edition for a static dataset", t, func() {
		publishedVersion := &models.Version{
			DatasetID:   "123-456",
			Edition:     "678",
			Version:     1,
			State:       models.PublishedState,
			ReleaseDate: "1996-04-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456",
					ID:   "123-456",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678",
					ID:   "678",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
					ID:   "1",
				},
			},
			LastUpdated: time.Date(2025, 3, 11, 0, 0, 0, 0, time.UTC),
		}

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
				return publishedVersion, nil
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, versionDoc *models.Version) error {
				return nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		Convey("When we call the GET edition endpoint", func() {
			api.Router.ServeHTTP(w, r)

			Convey("Then it returns a 200 OK", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("And the audit service is NOT called", func() {
				So(len(auditServiceMock.RecordVersionAuditEventCalls()), ShouldEqual, 0)
			})

			Convey("And the relevant calls have been made", func() {
				So(len(mockedDataStore.IsStaticDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestGetEditionAuditEventLogsErrorButContinues(t *testing.T) {
	t.Parallel()

	Convey("Given an authorised request where the audit service fails", t, func() {
		publishedVersion := &models.Version{
			DatasetID:   "123-456",
			Edition:     "678",
			Version:     1,
			State:       models.PublishedState,
			ReleaseDate: "1996-04-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456",
					ID:   "123-456",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678",
					ID:   "678",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123-456/editions/678/versions/1",
					ID:   "1",
				},
			},
			LastUpdated: time.Date(2025, 3, 11, 0, 0, 0, 0, time.UTC),
		}

		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			IsStaticDatasetFunc: func(ctx context.Context, datasetID string) (bool, error) {
				return true, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(ctx context.Context, datasetID, editionID, state string) (*models.Version, error) {
				if state == models.PublishedState {
					return publishedVersion, nil
				}
				return nil, errs.ErrVersionNotFound
			},
		}

		auditServiceMock := &applicationMocks.AuditServiceMock{
			RecordVersionAuditEventFunc: func(ctx context.Context, requestedBy models.RequestedBy, action models.Action, resource string, versionDoc *models.Version) error {
				return errors.New("audit service error")
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			RequireWithAttributesFunc: func(permission string, handlerFunc http.HandlerFunc, getAttributes authorisation.GetAttributesFromRequest) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "test-user-id"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{}, auditServiceMock)

		Convey("When we call the GET edition endpoint", func() {
			api.Router.ServeHTTP(w, r)

			Convey("Then it returns a 500 internal server error", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})

			Convey("And the audit service was called", func() {
				So(len(auditServiceMock.RecordVersionAuditEventCalls()), ShouldEqual, 1)
			})
		})
	})
}
