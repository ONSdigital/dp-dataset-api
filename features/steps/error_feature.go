package steps_test

import "fmt"

type ErrorFeature struct {
	err error
}

func (f *ErrorFeature) Errorf(format string, args ...interface{}) {
	f.err = fmt.Errorf(format, args...)
}

func (f *ErrorFeature) StepError() error {
	return f.err
}
