package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/sirupsen/logrus"
	"logagent/common"
	"time"
)

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed,err:%v\n", err)
		return
	}

	return
}

// 拉取日志收集的函数
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	//从etcd中读取key对应的json值
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed,err:%v", key, err)
		return
	}

	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 from etcd by key:%s failed,err:%v", key, err)
		return
	}

	ret := resp.Kvs[0]
	//json格式字符串
	fmt.Println(ret.Value)
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json unmarshal failed,err:%v", err)
		return
	}

	return
}
