package tailfile

import (
	"Log_Collection/Kafka"
	"Log_Collection/common"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

var (
	configChan chan []common.CollectEntry
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
}

func (t *tailTask) Init() (err error) {
	tailConfig := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.tailObj, err = tail.TailFile(t.path, tailConfig)
	return
}

// tailObj -> logs -> Client -> kafka
func (t *tailTask) run() {
	logrus.Infof("collect logs for %v is running", t.path)
	// 1. for{read data}
	for {
		line, ok := <-t.tailObj.Lines
		if !ok {
			logrus.Warnf("tail file close reopen,filename:%s\n", t.path)
			time.Sleep(time.Second)
			continue
		}
		// if line is null,stop sending the text to kafka
		// for windows: trim \r
		if len(strings.Trim(line.Text, "\r")) == 0 {
			continue
		}
		// change into asynchronous by channel
		// pack a line up into msg(type) for kafka,to channel
		msg := &sarama.ProducerMessage{}
		msg.Topic = t.topic
		msg.Value = sarama.StringEncoder(line.Text)
		// to a channel
		Kafka.ToMsgChan(msg)
	}
}

func newTailTask(path, topic string) *tailTask {
	task := &tailTask{
		path:  path,
		topic: topic,
	}
	return task
}

// tail
// There are many configs parts in allconf. Create a tailObj for every config part
func Init(allconf []common.CollectEntry) (err error) {
	for _, conf := range allconf {
		task := newTailTask(conf.Path, conf.Topic)
		err = task.Init()
		if err != nil {
			logrus.Errorf("tailfile: create tailObj for path:%s failed,err:%v", conf.Path, err)
			continue
		}
		// collect logs
		logrus.Infof("create a tail task for path:%v success!", conf.Path)
		go task.run()
	}
	// init configChan
	// init blocked channel(wait until new configs come)
	configChan = make(chan []common.CollectEntry)
	// wait for the coming of new config
	newConfig := <-configChan
	// Then manage tailTasks which launched before
	logrus.Infof("get new config from etcd,config:%v", newConfig)
	return
}

func SendNewConfig(newConfig []common.CollectEntry) {
	configChan <- newConfig
}
