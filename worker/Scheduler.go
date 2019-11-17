package worker

import (
	"../common"
	"fmt"
	"time"
)

// 任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent
	// 任务调度计划表
	jobPlanTable map[string]*common.JobSchedulePlan // 第一个是任务名称
	// 任务执行表，正在执行的任务放在这个表里边
	jobExecutingTable map[string]*common.JobExecuteInfo
	// 任务回传结果
	jobResultChan chan *common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件，将任务放在任务表中
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulerPlan *common.JobSchedulePlan
		err              error
		jobExisted       bool
		jobExecuteInfo   *common.JobExecuteInfo
		jobExecuting     bool
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulerPlan, err = common.BuildJobSchedulerPlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulerPlan
	case common.JOB_EVENT_DELETE:
		// 如果计划表中有任务，就将任务删除
		if jobSchedulerPlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消掉command执行
		// 判断任务是否在执行
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() // 取消任务执行，触发command杀死子进程，任务得到退出
		}
	}
}

// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度和执行是2件事情
	// 执行的任务可能会运行很久，例如 每个任务的运行时间是1分钟， 1分钟调度60次，但是只能执行一次；防止并发
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		// fmt.Println("正在执行：", jobPlan.Job.Name)
		return
	}
	// 构建任务执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)
	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo
	// 执行任务
	G_executor.ExecuteJob(jobExecuteInfo)
	fmt.Println("执行任务：", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
}

// 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)
	// 如果任务表为空，睡眠1秒
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}
	// 当前时间
	now = time.Now() // 一定要记着初始化！！！
	// 如果任务表不为空，遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		// 任务到期
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) { // 任务计划表中的任务应该在当前时间之前已经执行了
			// 尝试执行任务
			scheduler.TryStartJob(jobPlan)
			// fmt.Println("执行任务：", jobPlan.Job.Name)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 任务是周期性的，执行完成当前任务后，更新下一次的时间
		}
		// 统计最近一个要过期的任务时间，到达了之后再次调度任务
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	// 下次调度间隔 (最近要执行的任务调度时间 - 当前时间)
	scheduleAfter = (*nearTime).Sub(now)
	return
}

// 调度协程
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)
	// 初始化一次，先计算一次，这个肯定是1s
	scheduleAfter = scheduler.TrySchedule()

	// 调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	for {
		// select就是用来监听和channel有关的IO操作，当 IO 操作发生时，触发相应的动作
		select {
		case jobEvent = <-scheduler.jobEventChan: // 监听任务事件变化
			//fmt.Println(jobEvent.Job.Name)
			// 对内存中维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // 定时器最近的任务到期了，在这里等待定时器的时间！！ 注意select的用法
		case jobResult = <-scheduler.jobResultChan: // 监听任务执行结果
			scheduler.handleJobResult(jobResult)
			// 删除当前任务，重新调度，该任务的下一次时间就会添加上去
		}
		// 调度一次任务（当有新任务的操作，需要重新计算 || 定时器到期了，需要进入任务表执行最新的任务）
		scheduleAfter = scheduler.TrySchedule()
		// 重置定时器
		scheduleTimer.Reset(scheduleAfter)
	}
}

// 处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 从执行表中删除任务
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)
	fmt.Println("任务执行完成：", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED { // 不包含锁被占用的情况
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		// 将日志写到mongodb
		G_logSink.Append(jobLog)
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}

// 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000), // 1000长度的队列
	}
	// 启动调度协程
	go G_scheduler.scheduleLoop() // 为什么启动协程呢？如果不启动，一直在这里阻塞，后边主程序的初始化工作就无法进行下去
	return
}
