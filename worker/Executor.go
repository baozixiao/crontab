package worker

import (
	"../common"
	"context"
	"os/exec"
	"time"
)

type Executor struct {
}

// 执行一个任务
// 缺点：分布式场景中，一个任务会被执行多次------正确：一个任务应该被一个节点执行一次，worker节点在物理机上部署
// 当前worker节点正在执行任务，其他节点也可能执行同一个任务！！！
// 一个任务只能被一个worker节点所执行，当这个任务被该worker节点执行了，别的worker节点就不再执行这个任务，跳过
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)
		// 任务执行结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		// 如果抢到了锁，就执行shell
		// 如果没抢到锁，就跳过执行
		err = jobLock.TryLock()
		defer jobLock.UnLock() // 执行完毕之后，将锁释放，不再续约
		if err != nil {        // 上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else { // 上锁成功
			// 重置任务启动时间
			result.StartTime = time.Now()
			// 执行shell命令
			cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", info.Job.Command)
			//time.Sleep(10*time.Second)
			// 执行并捕获输出
			output, err = cmd.CombinedOutput()
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		// 将任务执行的结果返回给scheduler，scheduler会从executingTable中删除记录
		G_scheduler.PushJobResult(result)
	}()
}

var (
	G_executor *Executor
)

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
