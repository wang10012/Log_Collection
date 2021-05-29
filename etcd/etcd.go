package etcd

import (
	"Log_Collection/common"
	"Log_Collection/tailfile"
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// operations about etcd

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed,err:%v", err)
		return
	}
	return
}

// get config from etcd
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get config from etcd by key:%s failed,err:%v", key, err)
		return
	}
	// no data
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get no config from etcd by key:%s", key)
		return
	}
	res := resp.Kvs[0]
	fmt.Println(res.Value)
	err = json.Unmarshal(res.Value, collectEntryList)
	if err != nil {
		logrus.Errorf("json umnarsharl failed,err:%v", err)
		return
	}
	return
}

// watch etcd
func WatchConf(key string) {
	watchChan := client.Watch(context.Background(), key)
	var newConfig []common.CollectEntry
	for wresp := range watchChan {
		logrus.Infof("get now config from etcd!")
		for _, evt := range wresp.Events {
			fmt.Printf("type:%s,key:%s,value:%s", evt.Type, evt.Kv.Key, evt.Kv.Value)
			err := json.Unmarshal(evt.Kv.Value, &newConfig)
			if err != nil {
				logrus.Errorf("json unmarshal new config failed,err:%v", err)
			}
		}
		// communicate with tailfile by a channel;send new config
		tailfile.SendNewConfig(newConfig)
	}
}
