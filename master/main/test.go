package main

import (
	"../../common"
	"fmt"
)

func main() {
	//if err := master.InitConfig("master/main/master.json"); err != nil {
	//	fmt.Println("-------", err)
	//} else {
	//	fmt.Println(master.G_config)
	//}
	if resp, err := common.BuildResponse(0, "haha", "hahaha"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(resp))
	}

}
