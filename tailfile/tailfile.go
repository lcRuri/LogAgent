package tailfile

import (
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logagent/common"
)

type tailTask struct {
	path  string
	topic string
	tObj  *tail.Tail
}

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

func Init(allConf []common.CollectEntry) (err error) {

	//allConf存了若干个日志的收集项
	//针对每个日志收集项创建一个对应的tailObj
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		err := tt.Init()
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed,err:%v", conf.Path, err)
			continue
		}

	}

	if err != nil {
		logrus.Error("tailfile: create tailObj for path:%s failed,err:%v\n", filename, err)
		return
	}
	return
}
