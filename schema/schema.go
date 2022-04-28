package schema

import "github.com/ONSdigital/dp-kafka/v2/avro"

var generateCMDDownloads = `{
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

var generateCantabularDownloads = `{
  "type": "record",
  "name": "cantabular-export-start",
  "fields": [
    {"name": "instance_id", 		"type": "string", "default": ""},
    {"name": "dataset_id", 			"type": "string", "default": ""},
    {"name": "edition", 			"type": "string", "default": ""},
    {"name": "version", 			"type": "string", "default": ""},
	{"name": "filter_output_id", 	"type": "string", "default": ""}
  ]
}`

// GenerateCMDDownloadsEvent the Avro schema for FilterOutputSubmitted messages.
var GenerateCMDDownloadsEvent = &avro.Schema{
	Definition: generateCMDDownloads,
}

// GenerateCantabularDownloadsEvent the Avro schema for FilterOutputSubmitted messages.
var GenerateCantabularDownloadsEvent = &avro.Schema{
	Definition: generateCantabularDownloads,
}
