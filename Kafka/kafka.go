package Kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var(
	Client sarama.SyncProducer
)

// Init global kafka
func Init(address []string) (err error) {
	// 1. producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	// 2. connect kafka
	Client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("Kafka producer closed,err:", err)
		return
	}
	return
}
