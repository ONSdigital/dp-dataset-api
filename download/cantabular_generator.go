package download

import (
	"context"
	"github.com/ONSdigital/log.go/v2/log"
)

type CantabularGeneratorDownloads struct {
	InstanceID string `avro:"instance_id"`
	DatasetID  string `avro:"dataset_id"`
	Edition    string `avro:"edition"`
	Version    string `avro:"version"`
}

// Generator kicks off a full dataset version download task
type CantabularGenerator struct {
	Producer   KafkaProducer
	Marshaller GenerateDownloadsEvent
}

func (gen *CantabularGenerator) GetProducer() KafkaProducer {
	return gen.Producer
}

// Generate the full file download files for the specified dataset/edition/version
func (gen *CantabularGenerator) Generate(ctx context.Context, datasetID string, instanceID string, edition string, version string) error {
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

	// FilterID is set to an empty string as the avro schema expects there to be
	// a filter ID otherwise struct wont be marshalled into an acceptable message
	downloads := CantabularGeneratorDownloads{
		DatasetID:  datasetID,
		InstanceID: instanceID,
		Edition:    edition,
		Version:    version,
	}

	log.Info(ctx, "send cantabular generate downloads event", log.Data{
		"datasetID":  datasetID,
		"instanceID": instanceID,
		"edition":    edition,
		"version":    version,
	})

	avroBytes, err := gen.Marshaller.Marshal(downloads)
	if err != nil {
		return newGeneratorError(err, avroMarshalErr)
	}

	gen.Producer.Output() <- avroBytes

	return nil
}
