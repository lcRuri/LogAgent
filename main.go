package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/tailfile"
	"strings"
	"time"
)

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}
type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}
type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

func run() (err error) {
	// TailObj->log->Client->kafka
	for {
		line, ok := <-tailfile.TailObj.Lines
		if !ok {
			logrus.Warn("tail file close reopen,filename:%s\n", tailfile.TailObj.Filename)
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
		msg.Topic = "web_log"
		msg.Value = sarama.StringEncoder(line.Text)

		//丢到管道中
		kafka.ToMsgChan(msg)
	}
}

func main() {
	var configObj = new(Config)
	err := ini.MapTo(configObj, "./conf/conf.ini")
	if err != nil {
		logrus.Error("load config failed,err:%v", err)
		return
	}

	fmt.Printf("%#v\n", configObj)

	//初始化kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed,err:", err)
		return
	}

	logrus.Info("init kafka success!")

	//初始化etcd
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Error("init etcd failed,err:", err)
		return
	}

	//从etcd中拉取要收集日志的配置项
	allconf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Error("get conf from etcd failed,err:%v", err)
		return
	}
	fmt.Println(allconf)

	//初始化tail
	//将从etcd里面读取的配置项传入tail
	err = tailfile.Init(allconf)
	if err != nil {
		logrus.Error("init tailfile failed,err:", err)
		return
	}
	logrus.Info("init tailfile success!")

	//把日志通过sarama发往kafka
	err = run()
	if err != nil {
		logrus.Error("run failed,err:", err)
		return
	}
}
