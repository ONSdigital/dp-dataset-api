package api

import (
	kafka "github.com/ONSdigital/dp-kafka/v4"
)

type KafkaProducer interface {
	Output() chan kafka.BytesMessage
}
