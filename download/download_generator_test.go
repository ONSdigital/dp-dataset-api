package download

import (
	"github.com/ONSdigital/dp-dataset-api/download/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/clients/filter"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var (
	datasetID     = "111"
	edition       = "222"
	verisonID     = "333"
	versionNumber = "1"
	versionURL    = versionURI(datasetID, edition, versionNumber)
	filterID      = "666"
	errMock       = errors.New("borked")
	maxRetries    = 3
	xlsURL        = "/path/to/xls"
	csvURL        = "/path/to/csv"
)

func TestGenerator_GenerateDatasetDownloadsSuccess(t *testing.T) {
	Convey("Given no errors are returned from the filterClient or store", t, func() {
		model := filter.Model{
			FilterID: filterID,
			Downloads: map[string]filter.Download{
				xlsKey: {URL: xlsURL},
				csvKey: {URL: csvURL},
			},
		}

		v := &models.Version{ID: verisonID, Downloads: &models.DownloadList{}}

		storeMock := &mocks.StoreMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return v, nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
		}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return model, nil
			},
			UpdateBlueprintFunc: func(m filter.Model, doSubmit bool) (filter.Model, error) {
				return model, nil
			},
			GetOutputFunc: func(filterOutputID string) (filter.Model, error) {
				return model, nil
			},
		}

		Convey("When generateDownloads is called", func() {
			gen := Generator{filterCliMock, storeMock, 0, maxRetries}
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the correct calls are made to the filterClient", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 1)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 1)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 1)
			})

			Convey("And the correct calls are made to store", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 1)

				updateCalls := storeMock.UpdateVersionCalls()
				So(len(updateCalls), ShouldEqual, 1)
				So(updateCalls[0].Version.Downloads.XLS, ShouldResemble, &models.DownloadObject{URL: xlsURL})
				So(updateCalls[0].Version.Downloads.CSV, ShouldResemble, &models.DownloadObject{URL: csvURL})
				So(updateCalls[0].ID, ShouldEqual, verisonID)
			})
		})
	})
}

func TestGenerator_GenerateDatasetDownloadsUpdateVersionError(t *testing.T) {
	Convey("Given update version returns an error", t, func() {
		model := filter.Model{
			FilterID: filterID,
			Downloads: map[string]filter.Download{
				xlsKey: {URL: xlsURL},
				csvKey: {URL: csvURL},
			},
		}

		v := &models.Version{Downloads: &models.DownloadList{}}

		storeMock := &mocks.StoreMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return v, nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return errMock
			},
		}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return model, nil
			},
			UpdateBlueprintFunc: func(m filter.Model, doSubmit bool) (filter.Model, error) {
				return model, nil
			},
			GetOutputFunc: func(filterOutputID string) (filter.Model, error) {
				return model, nil
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, maxRetries}

		err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

		Convey("Then the expected error is returned", func() {
			So(err, ShouldResemble, newGeneratorError(errMock, updateDatasetVersionErr, versionURL))
		})

		Convey("And the correct calls are made to the filterClient", func() {
			So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
			So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 1)
			So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 1)
			So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 1)
		})

		Convey("And the correct calls are made to store", func() {
			So(len(storeMock.GetVersionCalls()), ShouldEqual, 1)
			So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 1)
		})
	})
}

func TestGenerator_GenerateDatasetDownloadsGetVersionError(t *testing.T) {
	Convey("Given get version returns an error", t, func() {
		model := filter.Model{
			FilterID: filterID,
			Downloads: map[string]filter.Download{
				xlsKey: {URL: xlsURL},
				csvKey: {URL: csvURL},
			},
		}

		storeMock := &mocks.StoreMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return nil, errMock
			},
		}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return model, nil
			},
			UpdateBlueprintFunc: func(m filter.Model, doSubmit bool) (filter.Model, error) {
				return model, nil
			},
			GetOutputFunc: func(filterOutputID string) (filter.Model, error) {
				return model, nil
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, maxRetries}
		err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

		Convey("Then the expected error is returned", func() {
			So(err, ShouldResemble, newGeneratorError(errMock, getVersionErr, versionURL))
		})

		Convey("And the correct calls are made to the filterClient", func() {
			So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
			So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 1)
			So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 1)
			So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 1)
		})

		Convey("And the correct calls are made to store", func() {
			So(len(storeMock.GetVersionCalls()), ShouldEqual, 1)
			So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
		})
	})
}

func TestGenerator_GenerateFullDatasetDownloadsRetriesExceeded(t *testing.T) {
	Convey("Given the  downloads are not available after the max number of retries", t, func() {
		model := filter.Model{FilterID: filterID}

		storeMock := &mocks.StoreMock{}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return model, nil
			},
			UpdateBlueprintFunc: func(m filter.Model, doSubmit bool) (filter.Model, error) {
				return model, nil
			},
			GetOutputFunc: func(filterOutputID string) (filter.Model, error) {
				return filter.Model{}, errMock
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, maxRetries}

		err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

		Convey("Then the expected error is returned", func() {
			So(err, ShouldResemble, newGeneratorError(nil, retriesExceededErr, versionURL))
		})

		Convey("And the correct calls are made to the filterClient", func() {
			So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
			So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 1)
			So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 1)
			So(len(filterCliMock.GetOutputCalls()), ShouldEqual, maxRetries)
		})

		Convey("And store is never called", func() {
			So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
			So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
		})
	})
}

func TestGenerator_GenerateFullDatasetDownloadsUpdateBlueprintError(t *testing.T) {
	Convey("Given an invalid parameters", t, func() {
		storeMock := &mocks.StoreMock{}

		jobState := filter.Model{FilterID: filterID}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return jobState, nil
			},
			UpdateBlueprintFunc: func(m filter.Model, doSubmit bool) (filter.Model, error) {
				return filter.Model{}, errMock
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 0}

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, newGeneratorError(errMock, updateBlueprintErr, filterID, versionURL))
			})

			Convey("And the correct calls are made to the filterClient", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 1)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 1)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestGenerator_GenerateFullDatasetDownloadsGetJobStateError(t *testing.T) {
	Convey("Given an invalid parameters", t, func() {
		storeMock := &mocks.StoreMock{}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return filter.Model{}, errMock
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 0}

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, newGeneratorError(errMock, getJobStateErr, filterID, versionURL))
			})

			Convey("And the correct calls are made to the filterClient", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 1)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestGenerator_GenerateFullDatasetDownloadsCreateBlueprintError(t *testing.T) {
	Convey("Given an invalid parameters", t, func() {
		storeMock := &mocks.StoreMock{}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return "", errMock
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 0}

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonID, versionNumber)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, newGeneratorError(errMock, createBlueprintErr, versionURL))
			})

			Convey("And the correct calls are made to the filterClient", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 1)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 0)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})
		})

	})
}

func TestGenerator_GenerateFullDatasetDownloadsValidationErrors(t *testing.T) {
	filterCliMock := &mocks.FilterClientMock{}

	storeMock := &mocks.StoreMock{}

	gen := Generator{filterCliMock, storeMock, 0, 0}

	Convey("Given an invalid datasetID", t, func() {

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads("", "", "", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, datasetIDEmptyErr)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})

			Convey("And filterClient is never called", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 0)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty edition", t, func() {
		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads("1234567890", "", "", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, editionEmptyErr)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})

			Convey("And filterClient is never called", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 0)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty versionID", t, func() {
		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads("1234567890", "edition", "", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, versionIDEmptyErr)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})

			Convey("And filterClient is never called", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 0)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an invalid version", t, func() {
		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads("1234567890", "edition", "0987654321", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, versionNumberInvalidErr)
			})

			Convey("And store is never called", func() {
				So(len(storeMock.GetVersionCalls()), ShouldEqual, 0)
				So(len(storeMock.UpdateVersionCalls()), ShouldEqual, 0)
			})

			Convey("And filterClient is never called", func() {
				So(len(filterCliMock.CreateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetJobStateCalls()), ShouldEqual, 0)
				So(len(filterCliMock.UpdateBlueprintCalls()), ShouldEqual, 0)
				So(len(filterCliMock.GetOutputCalls()), ShouldEqual, 0)
			})
		})
	})
}
