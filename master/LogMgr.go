package master

import (
	"../common"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

func (logMgr *LogMgr) ListLog(name string, skip int64, limit int64) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)
	logArr = make([]*common.JobLog, 0) // 初始化 len()=0
	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}
	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	// 发起查询
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Limit: &limit,
		Skip:  &skip,
		Sort:  logSort,
	}); err != nil {
		return
	}
	defer cursor.Close(context.TODO()) // 延迟释放游标

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		// 反序列化
		if err = cursor.Decode(jobLog); err != nil {
			continue // 有日志不合格
		}
		logArr = append(logArr, jobLog)
	}
	return
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)
	// 1.建立连接
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	if client, err = mongo.Connect(ctx, &options.ClientOptions{Hosts: []string{G_config.MongodbUri}}); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("my_db").Collection("my_collection"),
	}

	return
}
