package download

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	avroMarshalErr = "error while attempting to marshal generateDownloadsEvent to avro bytes"

	datasetIDEmptyErr  = newGeneratorError(nil, "failed to generate full dataset download as dataset ID was empty")
	instanceIDEmptyErr = newGeneratorError(nil, "failed to generate full dataset download as instance ID was empty")
	editionEmptyErr    = newGeneratorError(nil, "failed to generate full dataset download as edition was empty")
	versionEmptyErr    = newGeneratorError(nil, "failed to generate full dataset download as version was empty")
)

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
