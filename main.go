package main

import (
	"Log_Collection/Kafka"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

type config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
}

type KafkaConfig struct {
	Address string `ini:"address"`
	Topic   string `ini:"topic"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
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
	err = Kafka.Init([]string{configObj.Address})
	if err != nil {
		logrus.Error("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success!")
	// 3. init tail
}
