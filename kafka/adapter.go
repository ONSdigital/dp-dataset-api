package kafka

import kafka "github.com/ONSdigital/dp-kafka/v2"

// NewProducerAdapter creates a new kafka producer with access to Output function
func NewProducerAdapter(producer kafka.IProducer) *Producer {
	return &Producer{kafkaProducer: producer}
}

// Producer exposes an output function, to satisfy the interface used by go-ns libraries
type Producer struct {
	kafkaProducer kafka.IProducer
}

// Output returns the output channel
func (p Producer) Output() chan []byte {
	return p.kafkaProducer.Channels().Output
}
