package worker

import (
	"../common"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// 存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

// 批量插入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch // 当前的批次
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch // 超时批次
	)
	for {
		select {
		case log = <-logSink.logChan:
			// 将log写入
			// 每次插入需要等待mongodb的一次请求往返，耗时可能因为网络花费较长的时间
			if logBatch == nil {
				logBatch = &common.LogBatch{} // 初始化

				// 让这个机制超时自动提交（给1秒的时间） -> 某个场景下，很慢产生一条日志，batch很难变满
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					// 这里的逻辑是：执行func(batch *common.LogBatch)，会返回一个符合标准的新函数 fun()
					func(batch *common.LogBatch) func() {
						return func() {
							// 重点：发出超时通知，不要直接提交batch
							// 因为：这个回调函数是一个新的协程进行处理，存在并发写日志的问题
							logSink.autoCommitChan <- batch
						}
					}(logBatch)) // 超时执行回调函数，这个回调函数的返回值是一个函数
			}
			logBatch.Logs = append(logBatch.Logs, log)
			// 如果批次满了，就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				// 发送日志
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器 （感觉应该放到最前边！！！！！！！）
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan: // 过期的批次
			// 判断过期的批次 是否 让就是 当前的批次
			// 场景，批次满了，进行提交，清空 batch=nil 但是定时器还没有被stop，此时到达了定时器的时间，该batch被传给timeoutBatch
			// 个人感觉 这里不是很严谨！！！！
			if timeoutBatch != logBatch {
				continue // 跳过已经被提交的批次，结束当前for循环
			}
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}

var (
	G_logSink *LogSink
)

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)
	// 1.建立连接
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	if client, err = mongo.Connect(ctx, &options.ClientOptions{Hosts: []string{G_config.MongodbUri}}); err != nil {
		return
	}

	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("my_db").Collection("my_collection"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个mongodb处理协程
	go G_logSink.writeLoop()
	return
}
