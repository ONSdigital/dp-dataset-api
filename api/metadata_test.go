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

	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	cloudflareMocks "github.com/ONSdigital/dp-dataset-api/cloudflare/mocks"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
	. "github.com/smartystreets/goconvey/convey"
)

func TestPutMetadataForbidden(t *testing.T) {
	Convey("Given a request to put metdata is forbidden", t, func() {
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})

		w := httptest.NewRecorder()

		Convey("And a valid payload", func() {
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

			Convey("And a valid version", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", "123", "2017", "!")
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))

				Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					Convey("Then a 403 error is returned and no database calls are made", func() {
						So(w.Code, ShouldEqual, http.StatusForbidden)
						So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
						So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
						So(len(mockedDataStore.UpdateMetadataCalls()), ShouldEqual, 0)
					})
				})
			})
		})
	})
}

func TestPutMetadataUnauthorised(t *testing.T) {
	Convey("Given a request to put metdata is unauthorised", t, func() {
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})

		w := httptest.NewRecorder()

		Convey("And a valid payload", func() {
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

			Convey("And a valid version", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", "123", "2017", "!")
				r := createRequestWithNoAuth("PUT", url, bytes.NewBuffer(payload))

				Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					Convey("Then a 401 error is returned and no database calls are made", func() {
						So(w.Code, ShouldEqual, http.StatusUnauthorized)
						So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
						So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
						So(len(mockedDataStore.UpdateMetadataCalls()), ShouldEqual, 0)
					})
				})
			})
		})
	})
}

func TestPutMetadata(t *testing.T) {
	Convey("Given a version and a dataset stored in database", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})

		edition := version.Edition
		versionNo := strconv.Itoa(version.Version)

		w := httptest.NewRecorder()

		Convey("And a valid payload", func() {
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

			Convey("And an invalid version", func() {
				versionNo = "vvv"
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))

				Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					Convey("Then a 400 error is returned", func() {
						So(w.Code, ShouldEqual, http.StatusBadRequest)
						So(w.Body.String(), ShouldEqual, "invalid version requested\n")
					})
				})
			})

			Convey("And a valid version that does not exist", func() {
				versionNo = "88"
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))

				Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					Convey("Then a 404 error is returned", func() {
						So(w.Code, ShouldEqual, http.StatusNotFound)
						So(w.Body.String(), ShouldEqual, "version not found\n")
					})
				})
			})

			Convey("And an invalid version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Add("If-Match", "wrong-etag")

				Convey("When we call the PUT metadata endpoint", func() {
					api.Router.ServeHTTP(w, r)

					Convey("Then a 409 error is returned", func() {
						So(w.Code, ShouldEqual, http.StatusConflict)
						So(w.Body.String(), ShouldEqual, "instance does not match the expected eTag\n")
					})
				})
			})

			Convey("And a missing version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Del("If-Match") // no etag

				Convey("When we call the PUT metadata endpoint", func() {
					// Check metadata is changing
					So(dataset.Next.Title, ShouldNotEqual, metadata.Title)
					So(dataset.Next.Survey, ShouldNotEqual, metadata.Survey)
					So(version.ReleaseDate, ShouldNotEqual, metadata.ReleaseDate)
					So(version.LatestChanges, ShouldNotResemble, metadata.LatestChanges)

					api.Router.ServeHTTP(w, r)

					Convey("Then a 200 is returned and the metadata has changed", func() {
						So(w.Code, ShouldEqual, http.StatusOK)
						So(dataset.Next.Title, ShouldEqual, metadata.Title)
						So(dataset.Next.Survey, ShouldEqual, metadata.Survey)
						So(version.ReleaseDate, ShouldEqual, metadata.ReleaseDate)
						So(version.LatestChanges, ShouldResemble, metadata.LatestChanges)
					})
				})
			})

			Convey("And a star version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Add("If-Match", "*")

				Convey("When we call the PUT metadata endpoint", func() {
					// Check metadata is changing
					So(dataset.Next.Title, ShouldNotEqual, metadata.Title)
					So(dataset.Next.Survey, ShouldNotEqual, metadata.Survey)
					So(version.ReleaseDate, ShouldNotEqual, metadata.ReleaseDate)
					So(version.LatestChanges, ShouldNotResemble, metadata.LatestChanges)

					api.Router.ServeHTTP(w, r)

					Convey("Then a 200 is returned and the metadata has changed", func() {
						So(w.Code, ShouldEqual, http.StatusOK)
						So(dataset.Next.Title, ShouldEqual, metadata.Title)
						So(dataset.Next.Survey, ShouldEqual, metadata.Survey)
						So(version.ReleaseDate, ShouldEqual, metadata.ReleaseDate)
						So(version.LatestChanges, ShouldResemble, metadata.LatestChanges)
					})
				})
			})

			Convey("And a valid version etag", func() {
				url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
				r := createRequestWithAuth("PUT", url, bytes.NewBuffer(payload))
				r.Header.Add("If-Match", version.ETag)

				Convey("And a dataset id that does not exist", func() {
					dataset.ID = "changed"
					Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						Convey("Then a 404 error is returned", func() {
							So(w.Code, ShouldEqual, http.StatusNotFound)
							So(w.Body.String(), ShouldEqual, "dataset not found\n")
						})
					})
				})

				Convey("And the dataset does not have a next object", func() {
					dataset.Next = nil
					Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						Convey("Then a 500 error is returned", func() {
							So(w.Code, ShouldEqual, http.StatusInternalServerError)
							So(w.Body.String(), ShouldEqual, "internal error\n")
						})
					})
				})

				Convey("And the dataset is not associated", func() {
					dataset.Next.State = models.PublishedState
					Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						Convey("Then a 403 error is returned", func() {
							So(w.Code, ShouldEqual, http.StatusForbidden)
							So(w.Body.String(), ShouldEqual, "unable to update resource, expected resource to have a state of associated\n")
						})
					})
				})

				Convey("And the version is not associated", func() {
					version.State = models.PublishedState
					Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						Convey("Then a 403 error is returned", func() {
							So(w.Code, ShouldEqual, http.StatusForbidden)
							So(w.Body.String(), ShouldEqual, "unable to update resource, expected resource to have a state of associated\n")
						})
					})
				})

				Convey("And the UpdateMetadata call fails", func() {
					forceUpdateMetadataFail = true
					Convey("When we call the PUT metadata endpoint", func() {
						api.Router.ServeHTTP(w, r)

						Convey("Then a 500 error is returned", func() {
							So(w.Code, ShouldEqual, http.StatusInternalServerError)
							So(w.Body.String(), ShouldEqual, "internal error\n")
						})
					})
				})

				Convey("When we call the PUT metadata endpoint", func() {
					// Check metadata is changing
					So(dataset.Next.Title, ShouldNotEqual, metadata.Title)
					So(dataset.Next.Survey, ShouldNotEqual, metadata.Survey)
					So(version.ReleaseDate, ShouldNotEqual, metadata.ReleaseDate)
					So(version.LatestChanges, ShouldNotResemble, metadata.LatestChanges)

					api.Router.ServeHTTP(w, r)

					Convey("Then a 200 is returned and the metadata has changed", func() {
						So(w.Code, ShouldEqual, http.StatusOK)
						So(dataset.Next.Title, ShouldEqual, metadata.Title)
						So(dataset.Next.Survey, ShouldEqual, metadata.Survey)
						So(version.ReleaseDate, ShouldEqual, metadata.ReleaseDate)
						So(version.LatestChanges, ShouldResemble, metadata.LatestChanges)
					})
				})
			})
		})

		Convey("And an invalid payload", func() {
			payload := "invalid"
			url := fmt.Sprintf("http://localhost:22000/datasets/%s/editions/%s/versions/%s/metadata", dataset.ID, edition, versionNo)
			r := createRequestWithAuth("PUT", url, bytes.NewBufferString(payload))

			Convey("When we call the PUT metadata endpoint", func() {
				api.Router.ServeHTTP(w, r)

				Convey("Then a 400 error is returned", func() {
					So(w.Code, ShouldEqual, http.StatusBadRequest)
					So(w.Body.String(), ShouldEqual, "failed to parse json body\n")
				})
			})
		})
	})
}

func TestGetMetadataForbidden(t *testing.T) {
	t.Parallel()
	Convey("When a get request is made for metadata with incorrect authorisation details", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)
		Convey("Then a 403 response is returned and no expected database calls are made", func() {
			So(w.Code, ShouldEqual, http.StatusForbidden)

			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetMetadataUnauthorised(t *testing.T) {
	t.Parallel()
	Convey("When a get request is made for metadata without authorisation details", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)
		Convey("Then a 401 response is returned and no expected database calls are made", func() {
			So(w.Code, ShouldEqual, http.StatusUnauthorized)

			So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
			So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		})
	})
}

func TestGetMetadataReturnsOk(t *testing.T) {
	t.Parallel()
	var staticType = "static"
	Convey("Successfully return metadata resource for a request without an authentication header", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		responseBytes, err := io.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(responseBytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldBeNil)
		So(metaData.ReleaseFrequency, ShouldEqual, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
		So(metaData.State, ShouldEqual, versionDoc.State)
	})

	// Subtle difference between the test above and below, keywords is Not nil
	// in response for test below while it is nil in test above
	Convey("Successfully return metadata resource for a request with an authentication header", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		responseBytes, err := io.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(responseBytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldResemble, []string{"pensioners"})
		So(metaData.ReleaseFrequency, ShouldResemble, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
		So(metaData.State, ShouldEqual, versionDoc.State)
	})

	Convey("Successfully return metadata resource for a static dataset type", t, func() {
		datasetDoc := createDatasetDoc()
		datasetDoc.Current.Type = staticType
		if datasetDoc.Next != nil {
			datasetDoc.Next.Type = staticType
		}

		versionDoc := createPublishedVersionDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)

		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)

		responseBytes, err := io.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(responseBytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldBeNil)
		So(metaData.ReleaseFrequency, ShouldEqual, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
		So(metaData.State, ShouldEqual, versionDoc.State)
	})
}

func TestGetMetadataReturnsError(t *testing.T) {
	t.Parallel()
	var staticType = "static"
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrInternalServer
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return createDatasetDoc(), nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)

		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When the dataset document cannot be found return status not found", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document has no current sub document return status not found", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return nil, permissionsAPISDK.ErrFailedToParsePermissionsResponse
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document has no current or nextsub document but request is authorized return status internal server error", t, func() {
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
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, "internal error")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition document cannot be found for version return status not found", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When the version document cannot be found return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return createDatasetDoc(), nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the version document state is invalid return an internal server error", t, func() {
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an edition document for an invalid version is requested returns invalid version error", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/jjj/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When an edition document for version zero is requested return an invalid version error", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/0/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When an edition document for a negative version is requested return an invalid version error", t, func() {
		r := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/-1/metadata", nil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
			ParseFunc: func(token string) (*permissionsAPISDK.EntityData, error) {
				return &permissionsAPISDK.EntityData{UserID: "admin"}, nil
			},
		}

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInvalidVersion.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the version document for a static dataset cannot be found return status not found", t, func() {
		datasetDoc := createDatasetDoc()
		datasetDoc.Current.Type = staticType
		if datasetDoc.Next != nil {
			datasetDoc.Next.Type = staticType
		}

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrVersionNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 0)

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the edition document for a static dataset cannot be found return status not found", t, func() {
		datasetDoc := createDatasetDoc()
		datasetDoc.Current.Type = staticType
		if datasetDoc.Next != nil {
			datasetDoc.Next.Type = staticType
		}
		versionDoc := createPublishedVersionDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", http.NoBody)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return versionDoc, nil
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

		api := GetAPIWithCMDMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, SearchContentUpdatedProducer{}, &cloudflareMocks.ClienterMock{})
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrEditionNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
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
