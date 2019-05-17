package log

// Data can be used to include arbitrary key/value pairs
// in the structured log output.
//
// This should only be used where a predefined field isn't
// already available, since data included in a Data{} value
// isn't easily indexable.
//
// You can also create nested log data, for example:
//     Data {
//          "key": Data{},
//     }
type Data map[string]interface{}

func (d Data) attach(le *EventData) {
	le.Data = &d
}
