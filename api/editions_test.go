package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/utils"
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

func TestGetEditionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("get editions delegates offset and limit to db func and returns results list", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		publicResult := &models.Edition{ID: "20"}
		results := []*models.EditionUpdate{{Current: publicResult}}

		mockedDataStore := &storetest.StorerMock{
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		list, totalCount, err := api.getEditions(w, r, 20, 0)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(list, ShouldResemble, []*models.Edition{publicResult})
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})

	Convey("get published editions when the dataset type is static", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
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
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, totalCount, _ := api.getEditions(w, r, 20, 0)

		editions, err := utils.MapVersionsToEditionUpdate(publishedLatestVersion, nil)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetAllStaticVersionsCalls()), ShouldEqual, 1)
		So([]*models.EditionUpdate{editions}, ShouldEqual, editionsList)
		So(totalCount, ShouldEqual, 2)
		So(err, ShouldEqual, nil)
	})

	Convey("get all editions when the dataset type is static", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
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
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return nil
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

		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, permissions, permissions)
		_, totalCount, _ := api.getEditions(w, r, 20, 0)

		editions, err := utils.MapVersionsToEditionUpdate(publishedLatestVersion, unpublishedLatestVersion)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
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
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrInternalServer
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return "", nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(context.Context, string, string) error {
				return errs.ErrDatasetNotFound
			},
			GetDatasetTypeFunc: func(_ context.Context, _ string, authorised bool) (string, error) {
				return "", nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no editions exist against a dataset return version not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionsNotFound.Error())
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetAllStaticVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{}, nil
			},
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("A successful request to get edition when dataset is static returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return exampleStaticVersion, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return "", errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return "", errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("When edition is not published for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 0)
	})

	Convey("When dataset is static and version does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", http.NoBody)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetTypeFunc: func(context.Context, string, bool) (string, error) {
				return models.Static.String(), nil
			},
			GetLatestVersionStaticFunc: func(context.Context, string, string, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetLatestVersionStaticCalls()), ShouldEqual, 1)
	})
}
