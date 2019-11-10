package main

import (
	"../../master"
	"fmt"
)

func main() {
	if err := master.InitConfig("master/main/master.json"); err != nil {
		fmt.Println("-------", err)
	} else {
		fmt.Println(master.G_config)
	}
}
