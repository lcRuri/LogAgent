package tailfile

import (
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logagent/common"
	"logagent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	path  string
	topic string
	tObj  *tail.Tail
}

var (
	confChan chan []common.CollectEntry
)

func newTailTask(path, topic string) *tailTask {
	tt := &tailTask{
		path:  path,
		topic: topic,
	}

	return tt
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}

	t.tObj, err = tail.TailFile(t.path, cfg)
	return
}

// 读取日志
func (t *tailTask) run() {
	//读取日志发送给kafka
	// TailObj->log->Client->kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	for {
		line, ok := <-t.tObj.Lines
		if !ok {
			logrus.Warn("tail file close reopen,filename:%s\n", t.path)
			time.Sleep(1 * time.Second)
			continue
		}
		//避免空行
		if len(strings.Trim(line.Text, "\r")) == 0 {
			logrus.Info("出现空行，跳过...")
			continue
		}
		//利用通道将同步的代码改为异步的
		//把读出来的一行日志包装成kafka里面的msg类型，丢到通道中
		msg := &sarama.ProducerMessage{}
		msg.Topic = t.topic
		msg.Value = sarama.StringEncoder(line.Text)

		//丢到管道中
		kafka.ToMsgChan(msg)
	}
}

func SendNewConf(newConf []common.CollectEntry) {
	confChan <- newConf
}
