package master

import (
	"../common"
	"context"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// /cron/workers/
type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

func (workerMgr *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	var (
		getResp  *clientv3.GetResponse
		kv       *mvccpb.KeyValue
		workerIP string
	)
	workerArr = make([]string, 0)
	if getResp, err = workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	// 解析每个节点的ip
	for _, kv = range getResp.Kvs {
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}

var (
	G_workerMgr *WorkerMgr
)

func InitWorkerMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints, // etcd地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}
	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}
	// 得到KV和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}
