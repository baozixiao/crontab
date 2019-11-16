package main

import (
	"fmt"
	"runtime"
	"time"
)
import "../../worker"

func initEnv() {
	// 线程池中线程与 CPU 核心数量的对应关系
	runtime.GOMAXPROCS(runtime.NumCPU()) // 线程数量 = CPU数量
}

func main() {
	var (
		err error
	)
	// 初始化线程
	initEnv()
	// 加载配置
	if err = worker.InitConfig("worker/main/worker.json"); err != nil {
		goto ERR
	}
	// 启动调度器(先调度器，再管理器)
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	// 任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	// 不要走标签ERR，正常退出
	time.Sleep(time.Second * 1000)
	return

ERR:
	fmt.Println(err)
}
