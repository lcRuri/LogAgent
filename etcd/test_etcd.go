package main

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"time"
)

func main() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed,err:%v\n", err)
		return
	}

	fmt.Println("init etcd success")

	//watchCh := client.Watch(context.Background(), "s4")
	////阻塞监听是否key是否改变
	//for wresp := range watchCh {
	//	for _, evt := range wresp.Events {
	//		fmt.Printf("type:%s key:%s value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
	//	}
	//}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = client.Put(ctx, "s4", "what")
	if err != nil {
		fmt.Printf("put to etcd failed,err:%v\n", err)
		return
	}
	cancel()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	get, err := client.Get(ctx, "s4")
	if err != nil {
		fmt.Printf("get from etcd failed,err:%v\n", err)
		return
	}

	for _, kv := range get.Kvs {
		fmt.Printf("key:%s value:%s\n", kv.Value, kv.Value)
	}
	cancel()

	defer client.Close()
}
