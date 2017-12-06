package download

import (
	"testing"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	datasetID     = "111"
	edition       = "222"
	verisonID     = "333"
	versionNumber = "1"
	filterID      = "666"
	errMock       = errors.New("borked")
	maxRetries    = 3
	xlsURL        = "/path/to/xls"
	csvURL        = "/path/to/csv"
)

func TestGenerator_GenerateFullDatasetDownloadsValidationErrors(t *testing.T) {
	outputChan := make(chan []byte, 1)

	producerMock := &mocks.KafkaProducerMock{
		OutputFunc: func() chan []byte {
			return outputChan
		},
	}

	marhsallerMock := &mocks.GenerateDownloadsEventMock{
		MarshalFunc: func(s interface{}) ([]byte, error) {
			return nil, nil
		},
	}

	gen := Generator{
		Producer:   producerMock,
		Marshaller: marhsallerMock,
	}

	Convey("Given an invalid datasetID", t, func() {

		Convey("When the generator is called", func() {
			err := gen.Generate("", "", "", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, datasetIDEmptyErr)
			})

			Convey("And marshaller is never called", func() {
				So(len(marhsallerMock.MarshalCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty instanceID", t, func() {
		Convey("When the generator is called", func() {
			err := gen.Generate("1234567890", "", "", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, instanceIDEmptyErr)
			})

			Convey("And marshaller is never called", func() {
				So(len(marhsallerMock.MarshalCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty edition", t, func() {
		Convey("When the generator is called", func() {
			err := gen.Generate("1234567890", "1234567890", "", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, editionEmptyErr)
			})

			Convey("And marshaller is never called", func() {
				So(len(marhsallerMock.MarshalCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty version", t, func() {
		Convey("When the generator is called", func() {
			err := gen.Generate("1234567890", "1234567890", "time-series", "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, versionEmptyErr)
			})

			Convey("And marshaller is never called", func() {
				So(len(marhsallerMock.MarshalCalls()), ShouldEqual, 0)
			})

			Convey("And producer is never called", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 0)
			})
		})
	})
}
