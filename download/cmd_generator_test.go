package download

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var testContext = context.Background()

func TestGenerator_GenerateFullDatasetDownloadsValidationErrors(t *testing.T) {
	producerMock := &mocks.KafkaProducerMock{
		OutputFunc: func() chan []byte {
			return nil
		},
	}

	marhsallerMock := &mocks.GenerateDownloadsEventMock{
		MarshalFunc: func(s interface{}) ([]byte, error) {
			return nil, nil
		},
	}

	gen := CMDGenerator{
		Producer:   producerMock,
		Marshaller: marhsallerMock,
	}

	Convey("Given an invalid datasetID", t, func() {

		Convey("When the generator is called", func() {
			err := gen.Generate(testContext, "", "", "", "")

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
			err := gen.Generate(testContext, "1234567890", "", "", "")

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
			err := gen.Generate(testContext, "1234567890", "1234567890", "", "")

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
			err := gen.Generate(testContext, "1234567890", "1234567890", "time-series", "")

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

func TestGenerator_GenerateMarshalError(t *testing.T) {
	Convey("when marshal returns an error", t, func() {
		datasetID := "111"
		instanceID := "222"
		edition := "333"
		version := "4"
		mockErr := errors.New("let's get schwifty")

		producerMock := &mocks.KafkaProducerMock{
			OutputFunc: func() chan []byte {
				return nil
			},
		}

		marhsallerMock := &mocks.GenerateDownloadsEventMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return nil, mockErr
			},
		}

		gen := CMDGenerator{
			Producer:   producerMock,
			Marshaller: marhsallerMock,
		}

		err := gen.Generate(testContext, datasetID, instanceID, edition, version)

		Convey("then then expected error is returned", func() {
			So(err, ShouldResemble, newGeneratorError(mockErr, avroMarshalErr))
		})

		Convey("and marshal is called one time", func() {
			So(len(marhsallerMock.MarshalCalls()), ShouldEqual, 1)
		})

		Convey("and kafka producer is never called", func() {
			So(len(producerMock.OutputCalls()), ShouldEqual, 0)
		})
	})
}

func TestGenerator_Generate(t *testing.T) {
	Convey("given valid input", t, func() {
		datasetID := "111"
		instanceID := "222"
		edition := "333"
		version := "4"

		downloads := GenerateDownloads{
			FilterOutputID: "",
			DatasetID:      datasetID,
			InstanceID:     instanceID,
			Edition:        edition,
			Version:        version,
		}

		output := make(chan []byte, 1)
		avroBytes := []byte("hello world")

		producerMock := &mocks.KafkaProducerMock{
			OutputFunc: func() chan []byte {
				return output
			},
		}

		marhsallerMock := &mocks.GenerateDownloadsEventMock{
			MarshalFunc: func(s interface{}) ([]byte, error) {
				return avroBytes, nil
			},
		}

		gen := CMDGenerator{
			Producer:   producerMock,
			Marshaller: marhsallerMock,
		}

		Convey("when generate is called no error is returned", func() {
			err := gen.Generate(testContext, datasetID, instanceID, edition, version)
			So(err, ShouldBeNil)

			Convey("then marshal is called with the expected parameters", func() {
				So(len(marhsallerMock.MarshalCalls()), ShouldEqual, 1)
				So(marhsallerMock.MarshalCalls()[0].S, ShouldResemble, downloads)
			})

			Convey("and producer output is called one time with the expected parameters", func() {
				So(len(producerMock.OutputCalls()), ShouldEqual, 1)

				producerOut := <-output
				So(producerOut, ShouldResemble, avroBytes)
			})

		})
	})
}
