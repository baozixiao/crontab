package master

import (
	"../common"
	"encoding/json"
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
// 前端post一个json数据： job={"name":"job1", "command":"echo hello", "cronExpr":"* * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		postJob string
		job     *common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	fmt.Println("进入了handleJobSave方法内部")
	// 1. 解析post表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	// 2. 取表单中的job字段
	postJob = req.PostForm.Get("job") // 表单 key=job / value为json {"name":"job15","command":"echo hello1","cronExpr":"* * * * *"}
	// 3. 反序列化job
	job = &common.Job{}
	if err = json.Unmarshal([]byte(postJob), job); err != nil {
		goto ERR
	}
	// 4. 将任务job保存到 ETCD 中
	if oldJob, err = G_jobMgr.SaveJob(job); err != nil {
		goto ERR
	}
	// 5. 返回正常应答 {"errno":0, "msg":"", "data":{....}}
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return // 不进入ERR
ERR:
	fmt.Println(err)
	//6. 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
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
