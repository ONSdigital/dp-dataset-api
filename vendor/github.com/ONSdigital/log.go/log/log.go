package log

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/ONSdigital/go-ns/common"
	prettyjson "github.com/hokaccha/go-prettyjson"
)

// Namespace is the log namespace included with every log event.
//
// It defaults to the application binary name, but this should
// normally be set to a more sensible name on application startup
var Namespace = os.Args[0]

var destination io.Writer = os.Stdout
var fallbackDestination io.Writer = os.Stderr

var isTestMode bool

var eventWithOptionsCheckFunc = &eventFunc{eventWithOptionsCheck}
var eventWithoutOptionsCheckFunc = &eventFunc{eventWithoutOptionsCheck}
var eventFuncInst = initEvent()

var styleForHumanFunc = &styleFunc{styleForHuman}
var styleForMachineFunc = &styleFunc{styleForMachine}

// Event logs an event, to STDOUT if possible, or STDERR if not.
//
// Context can be nil.
//
// An event string should be static strings which do not use
// concatenation or Sprintf, e.g.
//     "connecting to database"
// rather than
//     "connecting to database: " + databaseURL
//
// Additional data should be stored using Data{}
//
// You can also pass in additional options which log extra event
// data, for example using the HTTP, Auth, Severity, Data and Error
// functions.
//
//     log.Event(nil, "connecting to database", log.Data{"url": databaseURL})
//
// If HUMAN_LOG environment variable is set to a true value (true, TRUE, 1)
// the log output will be syntax highlighted pretty printed JSON. Otherwise,
// the output is JSONLines format, with one JSON object per line.
//
// When running tests, Event will panic if the same option is passed
// in multiple times, for example:
//
//     log.Event(nil, "event", log.Data{}, log.Data{})
//
// It doesn't panic in normal usage because checking for duplicate entries
// is expensive. Where this happens, options to the right take precedence,
// for example:
//
//     log.Event(nil, "event", log.Data{"a": 1}, log.Data{"a": 2})
//     // data.a = 2
//
func Event(ctx context.Context, event string, opts ...option) {
	eventFuncInst.f(ctx, event, opts...)
}

func initEvent() *eventFunc {
	// If we're in test mode, replace the Event function with one
	// that has additional checks to find repeated event option types
	//
	// In test mode, a log event like this will result in a panic:
	//
	//    log.Event(nil, "demo", log.FATAL, log.WARN, log.ERROR)
	//
	// A flag called `test.v` is added by `go test`, so we can rely
	// on that to detect test mode.
	if flag.Lookup("test.v") != nil {
		isTestMode = true
		return eventWithOptionsCheckFunc
	}

	isTestMode = false
	return eventWithoutOptionsCheckFunc
}

var styler = initStyler()

func initStyler() *styleFunc {
	// If HUMAN_LOG is enabled, replace the default styler with a
	// human readable styler
	if b, _ := strconv.ParseBool(os.Getenv("HUMAN_LOG")); b {
		return styleForHumanFunc
	}

	return styleForMachineFunc
}

// eventFunc is a function which handles log events
type eventFunc struct {
	f func(ctx context.Context, event string, opts ...option)
}
type styleFunc = struct {
	f func(ctx context.Context, e EventData, ef eventFunc) []byte
}

// option is the interface which log options passed to eventFunc must match
//
// there's no point exporting this since it would require changes to the
// EventData struct (unless it forces data into log.Data or some other field,
// but we probably don't want that)
type option interface {
	attach(*EventData)
}

// EventData is the data structure used for logging an event
//
// It is the top level structure which contains all other log event data.
//
// It isn't very useful to export, other than for documenting the
// data structure it outputs.
type EventData struct {
	// Required fields
	CreatedAt time.Time `json:"created_at"`
	Namespace string    `json:"namespace"`
	Event     string    `json:"event"`

	// Optional fields
	TraceID  string    `json:"trace_id,omitempty"`
	SpanID   string    `json:"span_id,omitempty"`
	Severity *severity `json:"severity,omitempty"`

	// Optional nested data
	HTTP *EventHTTP `json:"http,omitempty"`
	Auth *eventAuth `json:"auth,omitempty"`
	Data *Data      `json:"data,omitempty"`

	// Error data
	Error *EventError `json:"error,omitempty"`
}

// eventWithOptionsCheck is the event function used when running tests, and
// will panic if the same log option is passed in multiple times
//
// It is only used during tests because of the runtime performance overhead
func eventWithOptionsCheck(ctx context.Context, event string, opts ...option) {
	var optMap = make(map[string]struct{})
	for _, o := range opts {
		t := reflect.TypeOf(o)
		p := fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
		if _, ok := optMap[p]; ok {
			panic("can't pass in the same parameter type multiple times: " + p)
		}
		optMap[p] = struct{}{}
	}

	eventWithoutOptionsCheckFunc.f(ctx, event, opts...)
}

// eventWithoutOptionsCheck is the event function used when we're not running tests
//
// It doesn't do any log options checks to minimise the runtime performance overhead
func eventWithoutOptionsCheck(ctx context.Context, event string, opts ...option) {
	print(styler.f(ctx, *createEvent(ctx, event, opts...), eventFunc{eventWithoutOptionsCheck}))
}

// createEvent creates a new event struct and attaches the options to it
func createEvent(ctx context.Context, event string, opts ...option) *EventData {
	e := EventData{
		CreatedAt: time.Now(),
		Namespace: Namespace,
		Event:     event,
	}

	if ctx != nil {
		e.TraceID = common.GetRequestId(ctx)
	}

	// loop around each log option and call its attach method, which takes care
	// of the association with the EventData struct
	for _, o := range opts {
		o.attach(&e)
	}

	return &e
}

// handleStyleError handles any errors from JSON marshalling in one of the styler functions
func handleStyleError(ctx context.Context, e EventData, ef eventFunc, b []byte, err error) []byte {
	if err != nil {
		// marshalling failed, so we'll log a marshalling error and use Sprintf
		// to get some kind of text representation of the log data
		//
		// other than out of memory errors, marshalling can only fail for an unsupported type
		// e.g. using log.Data and passing in an io.Reader
		//
		// to avoid this becoming recursive, only pass primitive types in this line (string, int, etc)
		//
		// note: Error(err) currently ignores this constraint, but it's expected that the `err`
		// 		 passed in by the caller will have come from json.Marshal or prettyjson.Marshal
		//       which don't marshal any non-marshallable types anyway
		ef.f(ctx, "error marshalling event data", Error(err), Data{"event_data": fmt.Sprintf("%+v", e)})

		// if we're in test mode, we'll also panic to cause tests to fail
		if isTestMode {
			// don't capture and reuse fmt.Sprintf output above for this, since that adds
			// a performance/memory overhead, and reuse is only required in test mode
			panic("error marshalling event data: " + fmt.Sprintf("%+v", e))
		}

		return []byte{}
	}

	return b
}

// styleForMachine renders the event data in JSONLine format
func styleForMachine(ctx context.Context, e EventData, ef eventFunc) []byte {
	b, err := json.Marshal(e)

	return handleStyleError(ctx, e, ef, b, err)
}

// styleForHuman renders the event data in a human readable format
func styleForHuman(ctx context.Context, e EventData, ef eventFunc) []byte {
	b, err := prettyjson.Marshal(e)

	return handleStyleError(ctx, e, ef, b, err)
}

func print(b []byte) {
	if len(b) == 0 {
		return
	}

	// try and write to stdout
	if n, err := fmt.Fprintln(destination, string(b)); n != len(b)+1 || err != nil {
		// if that fails, try and write to stderr
		if n, err := fmt.Fprintln(fallbackDestination, string(b)); n != len(b)+1 || err != nil {
			// if that fails, panic!
			//
			// also defer an os.Exit since the panic might be captured in a recover
			// block in the caller, but we always want to exit in this scenario
			//
			// Note: deferring an os.Exit makes this particular block untestable
			// using conventional `go test`. But it's a narrow enough edge case that
			// it probably isn't worth trying, and only occurs in extreme circumstances
			// (os.Stdout and os.Stderr both being closed) where unpredictable
			// behaviour is expected. It's not clear what a panic or os.Exit would do
			// in this scenario, or if our process is even still alive to get this far.
			defer os.Exit(1)
			panic("error writing log data: " + err.Error())
		}
	}
}
