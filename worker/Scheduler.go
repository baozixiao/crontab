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
	}
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
			// TODO: 尝试执行任务 -- 为什么是尝试？因为可能上一次的任务还没有结束
			fmt.Println("执行任务：", jobPlan.Job.Name)
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
		}
		// 调度一次任务（当有新任务的操作，需要重新计算 || 定时器到期了，需要进入任务表执行最新的任务）
		scheduleAfter = scheduler.TrySchedule()
		// 重置定时器
		scheduleTimer.Reset(scheduleAfter)
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
	}
	// 启动调度协程
	go G_scheduler.scheduleLoop()
	return
}
