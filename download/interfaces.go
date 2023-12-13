package download

import kafka "github.com/ONSdigital/dp-kafka/v4"

//go:generate moq -out ../mocks/generate_downloads_mocks.go -pkg mocks . KafkaProducer GenerateDownloadsEvent

// KafkaProducer sends an outbound kafka message
type KafkaProducer interface {
	Output() chan kafka.BytesMessage
}

// GenerateDownloadsEvent marshal the event into avro format
type GenerateDownloadsEvent interface {
	Marshal(s interface{}) ([]byte, error)
}
