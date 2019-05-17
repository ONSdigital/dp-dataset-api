package log

const (
	// FATAL is an option you can pass to Event to specify a severity of FATAL/0
	FATAL severity = 0
	// ERROR is an option you can pass to Event to specify a severity of ERROR/1
	ERROR severity = 1
	// WARN is an option you can pass to Event to specify a severity of WARN/2
	WARN severity = 2
	// INFO is an option you can pass to Event to specify a severity of INFO/3
	INFO severity = 3
)

// severity is the log severity level
//
// we don't export this because we don't want the caller
// to define their own severity levels
type severity int

func (s severity) attach(le *EventData) {
	le.Severity = &s
}
