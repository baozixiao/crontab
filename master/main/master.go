package main

import (
	"fmt"
	"runtime"
	"time"
)
import "../../master"

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
	if err = master.InitConfig("master/main/master.json"); err != nil {
		goto ERR
	}
	// 启动Api HTTP服务
	if err = master.InitApiServer(err); err != nil {
		goto ERR
	}
	// 不要走标签ERR，正常退出
	time.Sleep(time.Second * 100)
	return

ERR:
	fmt.Println(err)
}
