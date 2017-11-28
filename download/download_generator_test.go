package download

import (
	"github.com/ONSdigital/dp-dataset-api/download/mocks"
	"github.com/ONSdigital/go-ns/clients/filter"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var (
	filterID  = "666"
	mockError = errors.New("borked")
)

func TestGenerator_GenerateFullDatasetDownloadsRetriesExceeded(t *testing.T) {
	Convey("Given an invalid parameters", t, func() {
		datasetID := "111"
		edition := "222"
		verisonId := "333"
		versionNumber := 1

		storeMock := &mocks.StoreMock{}

		model := filter.Model{FilterID: filterID}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return model, nil
			},
			UpdateBlueprintFunc: func(m filter.Model, doSubmit bool) (filter.Model, error) {
				return model, mockError
			},
			GetOutputFunc: func(filterOutputID string) (filter.Model, error) {
				return filter.Model{}, mockError
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 3}

		Convey("When the generator is called", func() {
			gen.GenerateDatasetDownloads(datasetID, edition, verisonId, versionNumber)
		})

	})
}

func TestGenerator_GenerateFullDatasetDownloadsUpdateBlueprintError(t *testing.T) {
	Convey("Given an invalid parameters", t, func() {
		datasetID := "111"
		edition := "222"
		verisonId := "333"
		versionNumber := 1
		versionURL := versionURI(datasetID, edition, versionNumber)

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
				return filter.Model{}, mockError
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 0}

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonId, versionNumber)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, newGeneratorError(mockError, updateBlueprintErr, filterID, versionURL))
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
		datasetID := "111"
		edition := "222"
		verisonId := "333"
		versionNumber := 1
		versionURL := versionURI(datasetID, edition, versionNumber)

		storeMock := &mocks.StoreMock{}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return filterID, nil
			},
			GetJobStateFunc: func(filterID string) (filter.Model, error) {
				return filter.Model{}, mockError
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 0}

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonId, versionNumber)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, newGeneratorError(mockError, getJobStateErr, filterID, versionURL))
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
		datasetID := "111"
		edition := "222"
		verisonId := "333"
		versionNumber := 1
		versionURL := versionURI(datasetID, edition, versionNumber)

		storeMock := &mocks.StoreMock{}

		filterCliMock := &mocks.FilterClientMock{
			CreateBlueprintFunc: func(instanceID string, names []string) (string, error) {
				return "", mockError
			},
		}

		gen := Generator{filterCliMock, storeMock, 0, 0}

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonId, versionNumber)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, newGeneratorError(mockError, createBlueprintErr, versionURL))
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

	datasetID := ""
	edition := ""
	verisonId := ""
	versionNumber := 0

	Convey("Given an invalid datasetID", t, func() {

		Convey("When the generator is called", func() {
			err := gen.GenerateDatasetDownloads(datasetID, edition, verisonId, versionNumber)

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
			err := gen.GenerateDatasetDownloads("1234567890", edition, verisonId, versionNumber)

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
			err := gen.GenerateDatasetDownloads("1234567890", "edition", verisonId, versionNumber)

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
			err := gen.GenerateDatasetDownloads("1234567890", "edition", "0987654321", versionNumber)

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
