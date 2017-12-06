package schema

import "github.com/ONSdigital/go-ns/avro"

var generateDownloads = `{
  "type": "record",
  "name": "filter-output-submitted",
  "namespace": "",
  "fields": [
    {"name": "filter_output_id", "type": "string"},
    {"name": "instance_id", "type": "string"},
    {"name": "dataset_id", "type": "string"},
    {"name": "edition", "type": "string"},
    {"name": "version", "type": "string"}
  ]
}`

// FilterSubmittedEvent the Avro schema for FilterOutputSubmitted messages.
var GenerateDownloadsEvent = &avro.Schema{
	Definition: generateDownloads,
}
