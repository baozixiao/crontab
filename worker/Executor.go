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
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd    *exec.Cmd
			err    error
			output []byte
			result *common.JobExecuteResult
		)
		// 任务执行结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}
		result.StartTime = time.Now()
		// 执行shell命令
		cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", info.Job.Command)
		// 执行并捕获输出
		output, err = cmd.CombinedOutput()
		result.EndTime = time.Now()
		result.Output = output
		result.Err = err
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
