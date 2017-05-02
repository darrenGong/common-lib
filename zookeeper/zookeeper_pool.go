package zookeeper

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"errors"
	"github.com/samuel/go-zookeeper/zk"
)

type ZookeeperPool struct {
	sync.Mutex
	zkConnPool   map[string]*ZKConn
}

var (
	zookeeperPool	*ZookeeperPool

	CONN_TIMEOUT	= 5 * time.Second
)

func InitZookeeperPool() {
	zookeeperPool = &ZookeeperPool{
		zkConnPool:	make(map[string]*ZKConn),
	}
}

/*
	@ server: IP:PORT,IP:PORT
*/
func GetZKConn(server string) (*ZKConn, error) {
	if "" == server {
		return nil, errors.New("Param[server] is empty")
	}

	return zookeeperPool.getConn(server)
}

func getServerByZKConn(zkConn *ZKConn) (string, error) {
	zookeeperPool.Lock()
	defer zookeeperPool.Unlock()

	fmt.Printf("Get server by zookeeper connect[server: %v, state: %v]\n",
		zkConn.zkConn.Server(), zkConn.zkConn.State())
	for server, conn := range zookeeperPool.zkConnPool {
		if zkConn == conn {
			return server, nil
		}
	}

	return "", fmt.Errorf("Invalid connect[%v]", zkConn)
}

func (z *ZookeeperPool) addConn(server string, zkConn* ZKConn) error {
	z.Mutex.Lock()
	defer z.Mutex.Unlock()

	z.zkConnPool[server] = zkConn
	return nil
}

func (z *ZookeeperPool) getConn(server string) (*ZKConn, error) {
	z.Mutex.Lock()
	zkConn, ok := z.zkConnPool[server]
	z.Mutex.Unlock()

	if ok {
		if zkConn.zkConn.State() == zk.StateDisconnected {
			return z.newConn(server)
		}
	} else {
		return z.newConn(server)
	}

	return zkConn, nil
}

func (z *ZookeeperPool) newConn(server string) (*ZKConn, error) {

	conn, _, err := zk.Connect(strings.Split(server, ","), CONN_TIMEOUT)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect zookeeper server[server:%s, err:%v]", server, err)
	}

	zkConn := &ZKConn{
		zkConn: conn,
	}
	z.addConn(server, zkConn)

	return zkConn, nil
}
