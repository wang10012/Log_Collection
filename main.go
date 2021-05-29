package main

import (
	"Log_Collection/Kafka"
	"Log_Collection/etcd"
	"Log_Collection/tailfile"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

type config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address string `ini:"address"`
	//Topic    string `ini:"topic"`
	ChanSize int64 `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func run() {
	select {}
}

func main() {
	// 1. load config
	var configObj = new(config)
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed,err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	// 2. init: connect kafka
	err = Kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed,err:%v", err)
		return
	}
	logrus.Infof("init kafka success!")

	// 3. etcd
	// 3.1: init etcd
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed,err:%v", err)
		return
	}
	// 3.2: get config from etcd
	allconf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("get config from etcd failed,err:%v", err)
	}
	fmt.Println(allconf)
	// 3.3: launch a goroutine to watch etcd
	go etcd.WatchConf(configObj.EtcdConfig.CollectKey)
	// 4. init tail
	err = tailfile.Init(allconf) // send configs which come from etcd to tail Init
	if err != nil {
		logrus.Errorf("init tail failed,err:%v", err)
		return
	}
	logrus.Infof("init tailfile success!")
	run()
}
