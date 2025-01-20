package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/smartystreets/goconvey/convey"
)

func TestPutMetadata(t *testing.T) {
	convey.Convey("Given a version and a dataset stored in database", t, func() {
		version := createUnpublishedVersionDoc()
		version.ETag = "version-etag"
		version.Version = 1
		version.Edition = "2017"

		dataset := createDatasetDoc()
		dataset.ID = "123"
		dataset.Next.State = models.AssociatedState

		forceUpdateMetadataFail := false // Flag to make the UpdateMetadata function return an error
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(_ context.Context, _, _ string, versionNumber int, _ string) (*models.Version, error) {
				if versionNumber == version.Version {
					return version, nil
				}
				return nil, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(_ context.Context, datasetID string) (*models.DatasetUpdate, error) {
				if datasetID == dataset.ID {
					return dataset, nil
				}
				return nil, errs.ErrDatasetNotFound
			},
			UpdateMetadataFunc: func(_ context.Context, datasetId, versionId, versionEtag string, updatedDataset *models.Dataset, updatedVersion *models.Version) error {
				versionEtagMatches := versionEtag == "*" || versionEtag == version.ETag
				if datasetId != dataset.ID || versionId != version.ID || !versionEtagMatches || updatedDataset != dataset.Next || updatedVersion != version {
					return errors.New("invalid parameters")
				}

				if forceUpdateMetadataFail {
					return errors.New("failed to update metadata")
				}
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		edition := version.Edition
		versionNo := strconv.Itoa(version.Version)

		w := httptest.NewRecorder()

		convey.Convey("And a valid payload", func() {
			metadata := models.EditableMetadata{
				Title:       "new title",
				Survey:      "new survey",
				ReleaseDate: "new release date",
				LatestChanges: &[]models.LatestChange{
					{
						Name:        "change 1",
						Description: "change description",
					},
				},
			}
			payload, _ := json.Marshal(metadata)

			convey.Convey("And an invalid version", func() {
				versionNo = "vvv"
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))

				convey.Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					convey.Convey("Then a 400 error is returned", func() {
						convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
						convey.So(w.Body.String(), convey.ShouldEqual, "invalid version requested\n")
					})
				})
			})

			convey.Convey("And a valid version that does not exist", func() {
				versionNo = "88"
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))

				convey.Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					convey.Convey("Then a 404 error is returned", func() {
						convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
						convey.So(w.Body.String(), convey.ShouldEqual, "version not found\n")
					})
				})
			})

			convey.Convey("And an invalid version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Add("If-Match", "wrong-etag")

				convey.Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					convey.Convey("Then a 409 error is returned", func() {
						convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
						convey.So(w.Body.String(), convey.ShouldEqual, "instance does not match the expected eTag\n")
					})
				})
			})

			convey.Convey("And a missing version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Del("If-Match") // no etag

				convey.Convey("When we call the PUT metadata endpoint", func() {
					// Check metadata is changing
					convey.So(dataset.Next.Title, convey.ShouldNotEqual, metadata.Title)
					convey.So(dataset.Next.Survey, convey.ShouldNotEqual, metadata.Survey)
					convey.So(version.ReleaseDate, convey.ShouldNotEqual, metadata.ReleaseDate)
					convey.So(version.LatestChanges, convey.ShouldNotResemble, metadata.LatestChanges)

					api.Router.ServeHTTP(w, r)

					convey.Convey("Then a 200 is returned and the metadata has changed", func() {
						convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
						convey.So(dataset.Next.Title, convey.ShouldEqual, metadata.Title)
						convey.So(dataset.Next.Survey, convey.ShouldEqual, metadata.Survey)
						convey.So(version.ReleaseDate, convey.ShouldEqual, metadata.ReleaseDate)
						convey.So(version.LatestChanges, convey.ShouldResemble, metadata.LatestChanges)
					})
				})
			})

			convey.Convey("And a star version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Add("If-Match", "*")

				convey.Convey("When we call the PUT metadata endpoint", func() {
					// Check metadata is changing
					convey.So(dataset.Next.Title, convey.ShouldNotEqual, metadata.Title)
					convey.So(dataset.Next.Survey, convey.ShouldNotEqual, metadata.Survey)
					convey.So(version.ReleaseDate, convey.ShouldNotEqual, metadata.ReleaseDate)
					convey.So(version.LatestChanges, convey.ShouldNotResemble, metadata.LatestChanges)

					api.Router.ServeHTTP(w, r)

					convey.Convey("Then a 200 is returned and the metadata has changed", func() {
						convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
						convey.So(dataset.Next.Title, convey.ShouldEqual, metadata.Title)
						convey.So(dataset.Next.Survey, convey.ShouldEqual, metadata.Survey)
						convey.So(version.ReleaseDate, convey.ShouldEqual, metadata.ReleaseDate)
						convey.So(version.LatestChanges, convey.ShouldResemble, metadata.LatestChanges)
					})
				})
			})

			convey.Convey("And a valid version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Add("If-Match", version.ETag)

				convey.Convey("And a dataset id that does not exist", func() {
					dataset.ID = "changed"
					convey.Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						convey.Convey("Then a 404 error is returned", func() {
							convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
							convey.So(w.Body.String(), convey.ShouldEqual, "dataset not found\n")
						})
					})
				})

				convey.Convey("And the dataset does not have a next object", func() {
					dataset.Next = nil
					convey.Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						convey.Convey("Then a 500 error is returned", func() {
							convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
							convey.So(w.Body.String(), convey.ShouldEqual, "internal error\n")
						})
					})
				})

				convey.Convey("And the dataset is not associated", func() {
					dataset.Next.State = models.PublishedState
					convey.Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						convey.Convey("Then a 403 error is returned", func() {
							convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
							convey.So(w.Body.String(), convey.ShouldEqual, "unable to update resource, expected resource to have a state of associated\n")
						})
					})
				})

				convey.Convey("And the version is not associated", func() {
					version.State = models.PublishedState
					convey.Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						convey.Convey("Then a 403 error is returned", func() {
							convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
							convey.So(w.Body.String(), convey.ShouldEqual, "unable to update resource, expected resource to have a state of associated\n")
						})
					})
				})

				convey.Convey("And the UpdateMetadata call fails", func() {
					forceUpdateMetadataFail = true
					convey.Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						convey.Convey("Then a 500 error is returned", func() {
							convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
							convey.So(w.Body.String(), convey.ShouldEqual, "internal error\n")
						})
					})
				})

				convey.Convey("When we call the PUT metadata endpoint", func() {
					// Check metadata is changing
					convey.So(dataset.Next.Title, convey.ShouldNotEqual, metadata.Title)
					convey.So(dataset.Next.Survey, convey.ShouldNotEqual, metadata.Survey)
					convey.So(version.ReleaseDate, convey.ShouldNotEqual, metadata.ReleaseDate)
					convey.So(version.LatestChanges, convey.ShouldNotResemble, metadata.LatestChanges)

					api.Router.ServeHTTP(w, r)

					convey.Convey("Then a 200 is returned and the metadata has changed", func() {
						convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
						convey.So(dataset.Next.Title, convey.ShouldEqual, metadata.Title)
						convey.So(dataset.Next.Survey, convey.ShouldEqual, metadata.Survey)
						convey.So(version.ReleaseDate, convey.ShouldEqual, metadata.ReleaseDate)
						convey.So(version.LatestChanges, convey.ShouldResemble, metadata.LatestChanges)
					})
				})
			})
		})

		convey.Convey("And an invalid payload", func() {
			payload := "invalid"
			url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
			r := createRequestWithAuth("PUT", url, bytes.NewBufferString(payload))

			convey.Convey("When we call the PUT metadata endpoint", func() {
				api.Router.ServeHTTP(w, r)

				convey.Convey("Then a 400 error is returned", func() {
					convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
					convey.So(w.Body.String(), convey.ShouldEqual, "failed to parse json body\n")
				})
			})
		})
	})
}

func TestGetMetadataReturnsOk(t *testing.T) {
	t.Parallel()
	convey.Convey("Successfully return metadata resource for a request without an authentication header", t, func() {
		datasetDoc := createDatasetDoc()
		versionDoc := createPublishedVersionDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)

		responseBytes, err := io.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(responseBytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		convey.So(metaData.Keywords, convey.ShouldBeNil)
		convey.So(metaData.ReleaseFrequency, convey.ShouldEqual, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		convey.So(metaData.Temporal, convey.ShouldResemble, &[]models.TemporalFrequency{temporal})
		convey.So(metaData.UnitOfMeasure, convey.ShouldEqual, "Pounds Sterling")
	})

	// Subtle difference between the test above and below, keywords is Not nil
	// in response for test below while it is nil in test above
	convey.Convey("Successfully return metadata resource for a request with an authentication header", t, func() {
		datasetDoc := createDatasetDoc()
		versionDoc := createUnpublishedVersionDoc()

		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)

		responseBytes, err := io.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(responseBytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		convey.So(metaData.Keywords, convey.ShouldResemble, []string{"pensioners"})
		convey.So(metaData.ReleaseFrequency, convey.ShouldResemble, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		convey.So(metaData.Temporal, convey.ShouldResemble, &[]models.TemporalFrequency{temporal})
		convey.So(metaData.UnitOfMeasure, convey.ShouldEqual, "Pounds Sterling")
	})
}

func TestGetMetadataReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset document cannot be found return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset document has no current sub document return status not found", t, func() {
		datasetDoc := createDatasetDoc()
		versionDoc := createPublishedVersionDoc()
		datasetDoc.Current = nil

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the dataset document has no current or nextsub document but request is authorized return status internal server error", t, func() {
		datasetDoc := createDatasetDoc()
		versionDoc := createPublishedVersionDoc()
		datasetDoc.Current = nil
		datasetDoc.Next = nil

		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, "internal error")

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When the edition document cannot be found for version return status not found", t, func() {
		datasetDoc := createDatasetDoc()
		versionDoc := createPublishedVersionDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When the version document cannot be found return status not found", t, func() {
		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When the version document state is invalid return an internal server error", t, func() {
		datasetDoc := createDatasetDoc()

		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})

	convey.Convey("When an edition document for an invalid version is requested returns invalid version error", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/jjj/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When an edition document for version zero is requested return an invalid version error", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/0/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})

	convey.Convey("When an edition document for a negative version is requested return an invalid version error", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/-1/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 1)
		convey.So(permissions.Required.Calls, convey.ShouldEqual, 0)

		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 0)
	})
}

// createDatasetDoc returns a datasetUpdate doc containing minimal fields but
// there is a clear difference between the current and next sub documents
func createDatasetDoc() *models.DatasetUpdate {
	generalDataset := &models.Dataset{
		CollectionID:     "4321",
		ReleaseFrequency: "yearly",
		State:            models.PublishedState,
		UnitOfMeasure:    "Pounds Sterling",
	}

	nextDataset := *generalDataset
	nextDataset.CollectionID = "3434"
	nextDataset.Keywords = []string{"pensioners"}
	nextDataset.State = models.AssociatedState

	datasetDoc := &models.DatasetUpdate{
		ID:      "123",
		Current: generalDataset,
		Next:    &nextDataset,
	}

	return datasetDoc
}

func createPublishedVersionDoc() *models.Version {
	temporal := models.TemporalFrequency{
		EndDate:   "2017-05-09",
		Frequency: "Monthly",
		StartDate: "2014-05-09",
	}
	versionDoc := &models.Version{
		State:        models.PublishedState,
		CollectionID: "3434",
		Temporal:     &[]models.TemporalFrequency{temporal},
	}

	return versionDoc
}

func createUnpublishedVersionDoc() *models.Version {
	temporal := models.TemporalFrequency{
		EndDate:   "2017-05-09",
		Frequency: "Monthly",
		StartDate: "2014-05-09",
	}
	versionDoc := &models.Version{
		State:        models.AssociatedState,
		CollectionID: "3434",
		Temporal:     &[]models.TemporalFrequency{temporal},
	}

	return versionDoc
}
