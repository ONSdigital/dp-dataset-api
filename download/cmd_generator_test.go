package download

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/pkg/errors"
	"github.com/smartystreets/goconvey/convey"
)

var testContext = context.Background()

func TestGenerator_GenerateFullDatasetDownloadsValidationErrors(t *testing.T) {
	producerMock := &mocks.KafkaProducerMock{
		OutputFunc: func() chan kafka.BytesMessage {
			return nil
		},
	}

	marhsallerMock := &mocks.GenerateDownloadsEventMock{
		MarshalFunc: func(interface{}) ([]byte, error) {
			return nil, nil
		},
	}

	gen := CMDGenerator{
		Producer:   producerMock,
		Marshaller: marhsallerMock,
	}

	convey.Convey("Given an invalid datasetID", t, func() {
		convey.Convey("When the generator is called", func() {
			err := gen.Generate(testContext, "", "", "", "")

			convey.Convey("Then the expected error is returned", func() {
				convey.So(err, convey.ShouldResemble, datasetIDEmptyErr)
			})

			convey.Convey("And marshaller is never called", func() {
				convey.So(len(marhsallerMock.MarshalCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("And producer is never called", func() {
				convey.So(len(producerMock.OutputCalls()), convey.ShouldEqual, 0)
			})
		})
	})

	convey.Convey("Given an empty instanceID", t, func() {
		convey.Convey("When the generator is called", func() {
			err := gen.Generate(testContext, "1234567890", "", "", "")

			convey.Convey("Then the expected error is returned", func() {
				convey.So(err, convey.ShouldResemble, instanceIDEmptyErr)
			})

			convey.Convey("And marshaller is never called", func() {
				convey.So(len(marhsallerMock.MarshalCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("And producer is never called", func() {
				convey.So(len(producerMock.OutputCalls()), convey.ShouldEqual, 0)
			})
		})
	})

	convey.Convey("Given an empty edition", t, func() {
		convey.Convey("When the generator is called", func() {
			err := gen.Generate(testContext, "1234567890", "1234567890", "", "")

			convey.Convey("Then the expected error is returned", func() {
				convey.So(err, convey.ShouldResemble, editionEmptyErr)
			})

			convey.Convey("And marshaller is never called", func() {
				convey.So(len(marhsallerMock.MarshalCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("And producer is never called", func() {
				convey.So(len(producerMock.OutputCalls()), convey.ShouldEqual, 0)
			})
		})
	})

	convey.Convey("Given an empty version", t, func() {
		convey.Convey("When the generator is called", func() {
			err := gen.Generate(testContext, "1234567890", "1234567890", "time-series", "")

			convey.Convey("Then the expected error is returned", func() {
				convey.So(err, convey.ShouldResemble, versionEmptyErr)
			})

			convey.Convey("And marshaller is never called", func() {
				convey.So(len(marhsallerMock.MarshalCalls()), convey.ShouldEqual, 0)
			})

			convey.Convey("And producer is never called", func() {
				convey.So(len(producerMock.OutputCalls()), convey.ShouldEqual, 0)
			})
		})
	})
}

func TestGenerator_GenerateMarshalError(t *testing.T) {
	convey.Convey("when marshal returns an error", t, func() {
		datasetID := "111"
		instanceID := "222"
		edition := "333"
		version := "4"
		mockErr := errors.New("let's get schwifty")

		producerMock := &mocks.KafkaProducerMock{
			OutputFunc: func() chan kafka.BytesMessage {
				return nil
			},
		}

		marhsallerMock := &mocks.GenerateDownloadsEventMock{
			MarshalFunc: func(interface{}) ([]byte, error) {
				return nil, mockErr
			},
		}

		gen := CMDGenerator{
			Producer:   producerMock,
			Marshaller: marhsallerMock,
		}

		err := gen.Generate(testContext, datasetID, instanceID, edition, version)

		convey.Convey("then then expected error is returned", func() {
			convey.So(err, convey.ShouldResemble, newGeneratorError(mockErr, avroMarshalErr))
		})

		convey.Convey("and marshal is called one time", func() {
			convey.So(len(marhsallerMock.MarshalCalls()), convey.ShouldEqual, 1)
		})

		convey.Convey("and kafka producer is never called", func() {
			convey.So(len(producerMock.OutputCalls()), convey.ShouldEqual, 0)
		})
	})
}

func TestGenerator_Generate(t *testing.T) {
	convey.Convey("given valid input", t, func() {
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

		output := make(chan kafka.BytesMessage, 1)
		avroBytes := []byte("hello world")

		producerMock := &mocks.KafkaProducerMock{
			OutputFunc: func() chan kafka.BytesMessage {
				return output
			},
		}

		marhsallerMock := &mocks.GenerateDownloadsEventMock{
			MarshalFunc: func(interface{}) ([]byte, error) {
				return avroBytes, nil
			},
		}

		gen := CMDGenerator{
			Producer:   producerMock,
			Marshaller: marhsallerMock,
		}

		convey.Convey("when generate is called no error is returned", func() {
			err := gen.Generate(testContext, datasetID, instanceID, edition, version)
			convey.So(err, convey.ShouldBeNil)

			convey.Convey("then marshal is called with the expected parameters", func() {
				convey.So(len(marhsallerMock.MarshalCalls()), convey.ShouldEqual, 1)
				convey.So(marhsallerMock.MarshalCalls()[0].S, convey.ShouldResemble, downloads)
			})

			convey.Convey("and producer output is called one time with the expected parameters", func() {
				convey.So(len(producerMock.OutputCalls()), convey.ShouldEqual, 1)

				producerOut := <-output
				convey.So(producerOut.Value, convey.ShouldResemble, avroBytes)
			})
		})
	})
}
