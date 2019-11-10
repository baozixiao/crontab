package master

import (
	"encoding/json"
	"io/ioutil"
)

var (
	// 单例
	G_config *Config
)

// master.json配置文件
type Config struct {
	ApiPort         int      `json:"apiPort"`
	ApiReadTimeout  int      `json:"apiReadTimeout"`
	ApiWriteTimeout int      `json:"apiWriteTimeout"`
	EtcdEndPoints   []string `json:"etcdEndPoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
}

// 加载配置
func InitConfig(finename string) (err error) {
	var (
		content []byte
		conf    *Config
	)
	// 读取配置文件
	if content, err = ioutil.ReadFile(finename); err != nil {
		return
	}
	// json反序列化
	conf = &Config{}
	if err = json.Unmarshal(content, conf); err != nil {
		return
	}
	// 赋值单例
	G_config = conf
	return nil
}
