package log

import (
	"reflect"
	"runtime"
)

// EventError is the data structure used for logging a error event.
//
// It isn't very useful to export, other than for documenting the
// data structure it outputs.
type EventError struct {
	Error      string            `json:"error,omitempty"`
	StackTrace []EventStackTrace `json:"stack_trace,omitempty"`
	// This uses interface{} type, but should always be a type of kind struct
	// (which serialises to map[string]interface{})
	// See `func Error` switch block for more info
	Data interface{} `json:"data,omitempty"`
}

// EventStackTrace is the data structure used for logging a stack trace.
//
// It isn't very useful to export, other than for documenting the
// data structure it outputs.
type EventStackTrace struct {
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	Function string `json:"function,omitempty"`
}

func (l *EventError) attach(le *EventData) {
	le.Error = l
}

// Error returns an option you can pass to Event to attach
// error information to a log event
//
// It uses error.Error() to stringify the error value
//
// It also includes the error type itself as unstructured log
// data. For a struct{} type, it is included directly. For all
// other types, it is wrapped in a Data{} struct
//
// It also includes a full strack trace to where Error() is called,
// so you shouldn't normally store a log.Error for reuse (e.g. as a
// package level variable)
func Error(err error) option {
	e := &EventError{
		Error:      err.Error(),
		StackTrace: make([]EventStackTrace, 0),
	}

	k := reflect.Indirect(reflect.ValueOf(err)).Type().Kind()
	switch k {
	case reflect.Struct:
		// We've got a struct type, so make it the top level value
		e.Data = err
	default:
		// We have something else, so nest it inside a Data value
		e.Data = Data{"value": err}
	}

	pc := make([]uintptr, 10)
	n := runtime.Callers(2, pc)
	if n > 0 {
		frames := runtime.CallersFrames(pc[:n])

		for {
			frame, more := frames.Next()

			e.StackTrace = append(e.StackTrace, EventStackTrace{
				File:     frame.File,
				Line:     frame.Line,
				Function: frame.Function,
			})

			if !more {
				break
			}
		}
	}

	return e
}
