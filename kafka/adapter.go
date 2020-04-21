package kafka

import kafka "github.com/ONSdigital/dp-kafka"

// NewProducerAdapter creates a new kafka producer with access to Output function
func NewProducerAdapter(producer *kafka.Producer) *Producer {
	return &Producer{kafkaProducer: producer}
}

// Producer exposes an output function, to satisfy the interface used by go-ns libraries
type Producer struct {
	kafkaProducer *kafka.Producer
}

// Output returns the output channel
func (p Producer) Output() chan []byte {
	return p.kafkaProducer.Channels().Output
}
