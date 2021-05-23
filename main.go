package main

import (
	"Log_Collection/Kafka"
	"Log_Collection/tailfile"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"time"
)

type config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

// tailObj -> logs -> Client -> kafka
func run() (err error) {
	// 1. for{read data}
	for {
		line, ok := <-tailfile.TailObj.Lines
		if !ok {
			logrus.Warn("tail file close reopen,filename:%s\n", tailfile.TailObj.Filename)
			time.Sleep(time.Second)
			continue
		}
		// change into asynchronous by channel
		// pack a line up into msg(type) for kafka,to channel
		msg := &sarama.ProducerMessage{}
		msg.Topic = "web_log"
		msg.Value = sarama.StringEncoder(line.Text)
		// to a channel
		Kafka.MsgChan <- msg
	}

}

func main() {
	// 1. load config
	var configObj = new(config)
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Error("load config failed,err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	// 2. init: connect kafka
	err = Kafka.Init([]string{configObj.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success!")
	// 3. init tail
	err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	if err != nil {
		logrus.Error("init tail failed,err:%v", err)
		return
	}
	logrus.Info("init tailfile success!")
	// 4. send logs to kafka by tail
	err = run()
	if err != nil {
		logrus.Error("send logs to kafka failed,err:%v", err)
		return
	}
}
