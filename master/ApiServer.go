package master

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

var (
	G_apiServer *ApiServer // 单例对象，默认为nil，首字母大写，其他对象可以访问
)

// 任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

// 保存任务接口
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	fmt.Println("进入了handleJobSave方法内部")
	// 将任务保存到ETCD中
}

// 初始化服务
func InitApiServer(err error) error {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave) // 处理请求

	// 启动TCP监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil { // 本机任意ip的端口都可以
		fmt.Println(err)
		return err
	}

	// 创建一个HTTP服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond, // 单位为毫秒，所以要转换
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux, // 添加路由
	}

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	go httpServer.Serve(listener) // 启动了服务端

	return err
}
