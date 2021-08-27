package download

import (
	"context"
	"fmt"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

//go:generate moq -out ../mocks/generate_downloads_mocks.go -pkg mocks . KafkaProducer GenerateDownloadsEvent

var (
	avroMarshalErr = "error while attempting to marshal generateDownloadsEvent to avro bytes"

	datasetIDEmptyErr  = newGeneratorError(nil, "failed to generate full dataset download as dataset ID was empty")
	instanceIDEmptyErr = newGeneratorError(nil, "failed to generate full dataset download as instance ID was empty")
	editionEmptyErr    = newGeneratorError(nil, "failed to generate full dataset download as edition was empty")
	versionEmptyErr    = newGeneratorError(nil, "failed to generate full dataset download as version was empty")
)

// KafkaProducer sends an outbound kafka message
type KafkaProducer interface {
	Output() chan []byte
}

// GenerateDownloadsEvent marshal the event into avro format
type GenerateDownloadsEvent interface {
	Marshal(s interface{}) ([]byte, error)
}

type generateDownloads struct {
	FilterID   string `avro:"filter_output_id"`
	InstanceID string `avro:"instance_id"`
	DatasetID  string `avro:"dataset_id"`
	Edition    string `avro:"edition"`
	Version    string `avro:"version"`
}

// Generator kicks off a full dataset version download task
type Generator struct {
	Producer   KafkaProducer
	Marshaller GenerateDownloadsEvent
}

// Generate the full file download files for the specified dataset/edition/version
func (gen *Generator) Generate(ctx context.Context, datasetID string, instanceID string, edition string, version string) error {
	if datasetID == "" {
		return datasetIDEmptyErr
	}
	if instanceID == "" {
		return instanceIDEmptyErr
	}
	if edition == "" {
		return editionEmptyErr
	}
	if version == "" {
		return versionEmptyErr
	}

	// FilterID is set to an empty string as the avro schema expects there to be
	// a filter ID otherwise struct wont be marshalled into an acceptable message
	downloads := generateDownloads{
		FilterID:   "",
		DatasetID:  datasetID,
		InstanceID: instanceID,
		Edition:    edition,
		Version:    version,
	}

	log.Info(ctx, "send generate downloads event", log.Data{
		"datasetID":  datasetID,
		"instanceID": instanceID,
		"edition":    edition,
		"version":    version,
	})

	avroBytes, err := gen.Marshaller.Marshal(downloads)
	if err != nil {
		return newGeneratorError(err, avroMarshalErr)
	}

	gen.Producer.Output() <- avroBytes

	return nil
}

// GeneratorError is a wrapper for errors returned from the Generator
type GeneratorError struct {
	originalErr error
	message     string
	args        []interface{}
}

func newGeneratorError(err error, message string, args ...interface{}) GeneratorError {
	return GeneratorError{
		originalErr: err,
		message:     message,
		args:        args,
	}
}

// Error return details about the error
func (genErr GeneratorError) Error() string {
	if genErr.originalErr == nil {
		return errors.Errorf(genErr.message, genErr.args...).Error()
	}
	return errors.Wrap(genErr.originalErr, fmt.Sprintf(genErr.message, genErr.args...)).Error()
}
