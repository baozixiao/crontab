package worker

import (
	"../common"
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
)

// 分布式锁
type JobLock struct {
	// etcd客户端
	kv    clientv3.KV
	lease clientv3.Lease

	jobName    string             // 任务名称
	cancelFunc context.CancelFunc // 用于终止自动续租
	leaseID    clientv3.LeaseID   // 租约id
	isLocked   bool               // 是否上锁成功，每一个job都有一个JobLock对象，所以不存在冲突问题
}

// 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
		txn            clientv3.Txn
		lockKey        string
		txnResp        *clientv3.TxnResponse
	)
	// 1.创建租约 5秒
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	// 拿到租约的ID
	leaseId = leaseGrantResp.ID

	// 2.自动续租
	// 创建context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FALL
	}

	// 3.处理续租应答的协程，检测租约是否失效 以及 是否续租
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepRespChan:
				// 接受keepRespChan通道中的数据
				if keepResp == nil {
					fmt.Println("租约失效")
					goto END
				} else { // 每秒续租一次，所以会有一次应答
					fmt.Println("收到自动续租的应答: ", keepResp.ID)
				}
			}
		}
	END:
	}()

	// 4.创建事务
	txn = jobLock.kv.Txn(context.TODO())
	// 锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	// 5.事务抢锁
	/**
	1.利用租约在etcd集群中创建一个key，这个key有两种形态，存在和不存在，而这两种形态就是互斥量。
	2.如果这个key不存在，那么创建key，成功则获取到锁，该key就为存在状态。
	3.如果该key已经存在，那么线程就不能创建key，则获取锁失败。
	*/
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))). // key不存在，则创建锁，表示可以获取锁
		Else(clientv3.OpGet(lockKey))                                   // 否则抢锁失败

	// 提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto FALL
	}

	// 6.成功返回，失败则释放租约
	if !txnResp.Succeeded { // 锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FALL
	}
	// 抢锁成功
	jobLock.leaseID = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return

FALL:
	cancelFunc()                                  // 取消自动续租
	jobLock.lease.Revoke(context.TODO(), leaseId) // 释放租约
	return
}

// 释放锁，不再续约
func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()                                  // 取消自动租约的协程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseID) // 释放租约
	}
}

// 初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	return &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
}
