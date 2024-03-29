package download

import (
	"context"

	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/log.go/v2/log"
)

// Generator kicks off a full dataset version download task
type CMDGenerator struct {
	Producer   KafkaProducer
	Marshaller GenerateDownloadsEvent
}

type GenerateDownloads struct {
	FilterOutputID string `avro:"filter_output_id"`
	InstanceID     string `avro:"instance_id"`
	DatasetID      string `avro:"dataset_id"`
	Edition        string `avro:"edition"`
	Version        string `avro:"version"`
}

// Generate the full file download files for the specified dataset/edition/version
func (gen *CMDGenerator) Generate(ctx context.Context, datasetID, instanceID, edition, version string) error {
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

	// FilterOutputID is set to an empty string as the avro schema expects there to be
	// a filter output ID otherwise struct wont be marshalled into an acceptable message
	downloads := GenerateDownloads{
		FilterOutputID: "",
		DatasetID:      datasetID,
		InstanceID:     instanceID,
		Edition:        edition,
		Version:        version,
	}

	log.Info(ctx, "send CMD generate downloads event", log.Data{
		"datasetID":      datasetID,
		"instanceID":     instanceID,
		"edition":        edition,
		"version":        version,
		"FilterOutputId": "",
	})

	avroBytes, err := gen.Marshaller.Marshal(downloads)
	if err != nil {
		return newGeneratorError(err, avroMarshalErr)
	}

	gen.Producer.Output() <- kafka.BytesMessage{Value: avroBytes, Context: ctx}

	return nil
}
