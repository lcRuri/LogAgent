package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/tailfile"
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

func run() {
	select {}
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

	//开启一个goroutine去监听etcd里面key的变化
	go etcd.WatchConf(configObj.EtcdConfig.CollectKey)

	//初始化tail
	//将从etcd里面读取的配置项传入tail
	err = tailfile.Init(allconf)
	if err != nil {
		logrus.Error("init tailfile failed,err:", err)
		return
	}
	logrus.Info("init tailfile success!")

	run()
}
