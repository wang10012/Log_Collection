package tailfile

import (
	"Log_Collection/Kafka"
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
	ctx     context.Context
	cancel  context.CancelFunc
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
	for {
		select {
		case <-t.ctx.Done(): // when call the func:t.cancel
			logrus.Infof("path:%v is stopping", t.path)
			return
		// 1. for{read data}
		case line, ok := <-t.tailObj.Lines:
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
}

func newTailTask(path, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	task := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
	}
	return task
}
