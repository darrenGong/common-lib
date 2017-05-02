package main

import (
	"ucommon/zookeeper"
	"fmt"
	"log"
	"time"
	"unetfe-reconciliation/lib"
)

var (
	servers = "192.168.153.88:2181,192.168.153.89:2181,192.168.153.90:2181"
	path = "/NS/ulb/set666888/zookeepertest"
	absolute_path = path + "/1"

)

func main() {
	zookeeper.InitZookeeperPool()

	testConn, err := zookeeper.GetZKConn(servers)
	if err != nil {
		log.Fatalf("Failed to get conn[%v]", err)
	}

	value, err := testConn.GetNode(path)
	if err != nil {
		log.Printf("Failed to get node value[%v]\n", err)
	}
	fmt.Println(value)

	value = []byte("hello worle, zookeeper")
	if _, err := testConn.CreateNode(absolute_path, value); err != nil {
		log.Fatalf("Failed to create node[%v]", err)
		return
	}

	value, err = testConn.GetNode(path)
	if err != nil {
		log.Fatalf("Failed to get node value[%v]", err)
		return
	}
	fmt.Printf("%s", value)

	time.Sleep(5 * time.Second)

	value = []byte("hello worle, xiaotao")
	if _, err := testConn.SetNode(absolute_path, value); err != nil {
		log.Fatalf("Failed to set node[%s], err[%v]", path, err)
	}

	value, err = testConn.GetNode(path)
	if err != nil {
		log.Fatalf("Failed to get node value[%v]", err)
		return
	}
	fmt.Printf("%s", value)

	time.Sleep(20 * time.Second)

}
