package Kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client sarama.SyncProducer
	// for save memory,use *
	msgChan chan *sarama.ProducerMessage
)

// Init global kafka
func Init(address []string, ChanSize int64) (err error) {
	// 1. producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	// 2. connect kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("Kafka producer closed,err:", err)
		return
	}
	msgChan = make(chan *sarama.ProducerMessage, ChanSize)
	// kafka receive msg
	go sendMsg()
	return
}

//read msg from MsgChan,send to kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg to kafka failed,err:", err)
				return
			}
			logrus.Infof("send msg to kafka success! pid:%v offset:%v", pid, offset)
		}
	}
}

// Reduce exposure to variables
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
