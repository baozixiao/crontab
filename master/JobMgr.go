package master

import (
	"../common"
	"context"
	"encoding/json"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) { // 为其添加一个SaveJob方法
	// 将任务保存到 /cron/jobs/任务名 -> json
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj *common.Job
	)
	jobKey = common.JOB_SAVE_DIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil { // 将job变成json类型，作为value
		return
	}
	// 保存到etcd, withpreKV()表示 若为更新操作，将old值返回
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	// 如果为更新，返回旧值
	if putResp.PrevKv != nil {
		// 对旧值进行反序列化
		oldJobObj = &common.Job{}
		if err = json.Unmarshal(putResp.PrevKv.Value, oldJobObj); err != nil {
			return
		} else {
			return oldJobObj, nil
		}
	}
	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		oldJobObj *common.Job
		delResp   *clientv3.DeleteResponse
	)
	jobKey = common.JOB_SAVE_DIR + name // etcd中保存任务的key
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}
	// 返回被删除的任务信息
	oldJobObj = &common.Job{}
	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, oldJobObj); err != nil {
			return
		}
		return oldJobObj, nil
	}
	return
}

// 删除任务
func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		value   *mvccpb.KeyValue
	)
	dirKey = common.JOB_SAVE_DIR
	// 目录下所有任务信息
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}
	// 初始化数组空间，默认为nil
	jobList = make([]*common.Job, 0)
	for _, value = range getResp.Kvs {
		job := &common.Job{}
		if err = json.Unmarshal(value.Value, job); err != nil {
			err = nil
			continue // 忽视反序列化错误
		}
		jobList = append(jobList, job) // 追加元素，必须赋值，不要忘记了！！！！
	}
	return
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化任务管理器
func InitJobMgr() (err error) {
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

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return nil
}
