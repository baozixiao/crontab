package worker

import (
	"../common"
	"context"
	"go.etcd.io/etcd/clientv3"
	"net"
	"time"
)

// 注册当前节点到etcd /cron/workers/IP地址
type Register struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIP string // 本机ip
}

// 自动注册到etcd
func (register *Register) keepOnline() {
	var (
		regKey         string
		err            error
		leaseGrantResp *clientv3.LeaseGrantResponse
		keepAliveChan  <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp  *clientv3.LeaseKeepAliveResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
	)

	for {
		regKey = common.JOB_WORKER_DIR + register.localIP
		cancelFunc = nil
		// 创建10秒租约
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			// 如果创建租约失败，则休息后重试
			goto RETRY
		}
		// 自动续租 每秒续租一次
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		// 注册到etcd
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		// 如果服务正常，就在这里卡住了，因为一直会有续租

		// 处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil { //续租失败，超过了租约的时间，key就会消失
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}

}

var (
	G_register *Register
)

// 获取本机网卡ip
func getLocalIP() (ipv4 string, err error) {
	var (
		addrs   []net.Addr
		addr    net.Addr
		ipNet   *net.IPNet // ip地址
		isIpNet bool
	)
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	// 取第一个非lo的网卡
	for _, addr = range addrs {
		// ipv4 ipv6 进行反解为IPNet类型
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			// 这个网络地址是ip地址
			// 跳过ipv6
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

func InitRegister() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		localIP string
	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints, // etcd地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}
	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}
	// 得到KV和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	if localIP, err = getLocalIP(); err != nil {
		return
	}

	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIP,
	}

	// 服务注册
	go G_register.keepOnline()

	return
}
