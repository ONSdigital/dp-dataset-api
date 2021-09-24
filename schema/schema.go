package schema

import "github.com/ONSdigital/dp-kafka/v2/avro"

var generateDownloads = `{
  "type": "record",
  "name": "filter-output-submitted",
  "fields": [
    {"name": "filter_output_id", "type": "string", "default": ""},
    {"name": "instance_id", "type": "string", "default": ""},
    {"name": "dataset_id", "type": "string", "default": ""},
    {"name": "edition", "type": "string", "default": ""},
    {"name": "version", "type": "string", "default": ""}
  ]
}`

// GenerateDownloadsEvent the Avro schema for FilterOutputSubmitted messages.
var GenerateDownloadsEvent = &avro.Schema{
	Definition: generateDownloads,
}
