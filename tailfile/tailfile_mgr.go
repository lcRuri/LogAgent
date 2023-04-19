package tailfile

import (
	"github.com/sirupsen/logrus"
	"logagent/common"
)

type tailTaskMgr struct {
	tailTaskMap map[string]*tailTask
	//所有配置项
	collectEntryList []common.CollectEntry
	//等待新配置的通道
	confChan chan []common.CollectEntry
}

var (
	ttMgr *tailTaskMgr
)

func Init(allConf []common.CollectEntry) (err error) {
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry),
	}

	//allConf存了若干个日志的收集项
	//针对每个日志收集项创建一个对应的tailObj
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		err := tt.Init()
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed,err:%v", conf.Path, err)
			continue
		}

		//去收集日志
		logrus.Infof("create a tail task for path:%s success", conf.Path)
		//把创建的tailtask存储到ttMgr
		ttMgr.tailTaskMap[tt.path] = tt

		go tt.run()
	}

	//在后台监听新的配置
	go ttMgr.watch()

	return
}

func (t *tailTaskMgr) watch() {
	//接受新配置
	newConf := <-t.confChan
}
