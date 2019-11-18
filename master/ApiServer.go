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
	fmt.Println(postJob)
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

// 删除任务接口
// post /job/delete name = job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	// 删除的任务名称
	name = req.PostForm.Get("name")
	// 删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "succrss", oldJob); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	fmt.Println(err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 查看etcd中所有的任务列表
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		jobList []*common.Job
		bytes   []byte
	)
	if jobList, err = G_jobMgr.ListJob(); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	fmt.Println(err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 强制杀死某个任务
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err   error
		name  string
		bytes []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	// 要杀死的任务名称
	name = req.PostForm.Get("name")
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	fmt.Println(err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 任务日志，一个任务可能有多个日志 因为是周期性执行的
func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	var (
		err        error
		name       string // 任务名称
		skipParam  string // 从第几条开始
		limitParam string // 返回多少条
		skip       int
		limit      int
		logArr     []*common.JobLog
		bytes      []byte
	)
	// 解析Get参数
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	// 获取请求参数 /job/log?name=job10&skip=0&limit=10
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20 //默认给20条
	}
	if logArr, err = G_logMgr.ListLog(name, int64(skip), int64(limit)); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	fmt.Println(err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 服务发现模块，返回所有节点
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)
	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		resp.Write(bytes)
	}
	return
ERR:
	fmt.Println(err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 初始化服务
func InitApiServer(err error) error {
	var (
		mux           *http.ServeMux
		listener      net.Listener
		httpServer    *http.Server
		staticDir     http.Dir     // 静态文件根目录
		staticHandler http.Handler //静态文件的http回调
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave) // 处理请求
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog) // 日志查询
	mux.HandleFunc("/worker/list", handleWorkerList)

	staticDir = http.Dir(G_config.Webroot) // 静态文件目录  相对地址，相对于当前项目来说的！！！！
	staticHandler = http.FileServer(staticDir)
	// http.HandleFunc 该方法接收两个参数，一个是路由匹配的字符串，另外一个是 func(ResponseWriter, *Request) 类型的函数
	// http.Handle  接收两个参数，一个是路由匹配的字符串，另外一个是 Handler 类型的值
	mux.Handle("/", http.StripPrefix("/", staticHandler)) // 静态文件匹配下来，请求 /index.html，去掉前缀/

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
