package common

const (
	JOB_SAVE_DIR = "/cron/jobs/"

	// 任务强杀目录
	JOB_KILLER_DIR = "/cron/killer/"

	// 锁路径
	JOB_LOCK_DIR = "/cron/lock/"

	// 保存任务事件
	JOB_EVENT_SAVE = 1
	// 删除任务事件
	JOB_EVENT_DELETE = 2
)
