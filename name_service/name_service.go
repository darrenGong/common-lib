package nameservice

import (
	"sync"
	"strings"
	"errors"
	"math/rand"
	"log"
	"ucommon/zookeeper"
)

type NameContainer struct {
	sync.RWMutex
	MapNNC        map[string][]byte
	MapNameValues map[string][]string
}

func NewNameContainer() *NameContainer {
	nc := new(NameContainer)
	nc.MapNNC = make(map[string][]byte)
	nc.MapNameValues = make(map[string][]string)
	return nc
}

func (nc *NameContainer) SetName(shortName, fullName string, value []byte) {
	nc.Lock()
	defer nc.Unlock()
	nc.MapNNC[fullName] = value
	nc.MapNameValues[shortName] = append(nc.MapNameValues[shortName], fullName)
}

func (nc *NameContainer) DeleteName(shortName string) {
	nc.Lock()
	defer nc.Unlock()

	fullNames, ok := nc.MapNameValues[shortName]
	if !ok {
		log.Printf("The map have not shortname[%s], map[%v]", shortName, nc.MapNameValues)
		return
	}

	for _, fullName := range fullNames {
		delete(nc.MapNNC, fullName)
	}
}

func (nc *NameContainer) GetName(shortName string) ([]byte, error) {
	nc.RLock()
	defer nc.RUnlock()

	if _, ok := nc.MapNameValues[shortName]; !ok {
		return nil, errors.New("shortname to zk node empty")
	}
	if len(nc.MapNameValues[shortName]) == 0 {
		return nil, errors.New("shortname length to zk node empty")
	}
	num := rand.Intn(len(nc.MapNameValues[shortName]))
	full_name := nc.MapNameValues[shortName][num]
	if full_name == "" {
		log.Printf("[name_container]no full name for short name %s", shortName)
		return nil, errors.New("no full name")
	}
	values, ok := nc.MapNNC[full_name]
	if !ok {
		log.Printf("[name_container]no name node content for full name %s", full_name)
		return nil, errors.New("no name node")
	}

	return values, nil
}

func (nc *NameContainer) GetMaster(shortName string) ([]byte, error) {
	nc.RLock()
	defer nc.RUnlock()

	if _, ok := nc.MapNameValues[shortName]; !ok {
		return nil, errors.New("shortname to zk node empty")
	}
	if len(nc.MapNameValues[shortName]) == 0 {
		return nil, errors.New("shortname length to zk node empty")
	}
	for _, full_name := range nc.MapNameValues[shortName] {
		if strings.HasSuffix(full_name, "master") {
			if full_name == "" {
				log.Printf("no full name for short name %s", shortName)
				return nil, errors.New("no full name")
			}

			values, ok := nc.MapNNC[full_name]
			if !ok {
				log.Printf("no name node content for full name %s", full_name)
				return nil, errors.New("no name node")
			}

			return values, nil
		}
	}
	return nil, errors.New("no master node found")
}

func (nc *NameContainer) GetNameBatch(shortName string) ([][]byte, error) {
	nc.RLock()
	defer nc.RUnlock()
	if _, ok := nc.MapNameValues[shortName]; !ok {
		return nil, errors.New("shortname to zk node empty")
	}
	if len(nc.MapNameValues[shortName]) == 0 {
		return nil, errors.New("shortname length to zk node empty")
	}
	valuess := make([][]byte, 0, len(nc.MapNameValues[shortName]))
	for _, full_name := range nc.MapNameValues[shortName] {
		node := nc.MapNNC[full_name]
		valuess = append(valuess, node)
	}
	return valuess, nil
}

func (nc *NameContainer) ClearNameValues(shortName string) {
	nc.Lock()
	defer nc.Unlock()
	nc.MapNameValues[shortName] = make([]string, 0)
}

func (nc *NameContainer) SetNameBatch(shortName string, fullNames []string, valuess [][]byte) {
	nc.Lock()
	defer nc.Unlock()
	nc.MapNameValues[shortName] = make([]string, 0)
	if len(fullNames) == 0 || len(valuess) == 0 {
		return
	}
	for k, _ := range fullNames {
		nc.MapNNC[fullNames[k]] = valuess[k]
		nc.MapNameValues[shortName] = append(nc.MapNameValues[shortName], fullNames[k])
	}
}

func (nc *NameContainer) FetchZkName(connStr, shortName, fullName string) error {
	zkConn, err := zookeeper.GetZKConn(connStr)
	if err != nil {
		log.Printf("get zk instance error[%v]", err)
		return err
	}
	childs, ch, err := zkConn.ListChildrenWatch(fullName)
	if err != nil {
		log.Printf("get children[%s] instance error", fullName)
		return err
	}
	fullNames := make([]string, 0)
	nameNodes := make([][]byte, 0)
	for _, v := range childs {
		full_node := fullName + "/" + v
		data, err := zkConn.GetNodeAbsolutePath(full_node)
		if err != nil {
			log.Printf("get child data err[%v]", err)
			continue
		}

		if nil == data {
			log.Printf("The child[%s] data is empty", full_node)
			continue
		}

		fullNames = append(fullNames, full_node)
		nameNodes = append(nameNodes, data)
	}
	nc.SetNameBatch(shortName, fullNames, nameNodes)
	go func() {
		select {
		case ev := <-ch:
			log.Println("cat watcher", ev)
			nc.FetchZkName(connStr, shortName, fullName)
		}
	}()
	return nil
}

var (
	nameContainer = NewNameContainer()
)

func InitNameService(zkServer string, namelst map[string]string) {
	zookeeper.InitZookeeperPool()
	updateNames(zkServer, namelst)
	return
}

func updateNames(zkServer string, namelst map[string]string) {
	for k, v := range namelst {
		short_name := k
		full_name := v
		err := nameContainer.FetchZkName(zkServer, short_name, full_name)
		if err != nil {
			log.Printf("[update_names]fetch node name error[%v]", err)
			continue
		}
	}
	log.Printf("[update_names]name container info[%v]", nameContainer)
	return
}

func AddNameService(zkServer string, shortname, fullname string) {
	updateNames(zkServer, map[string]string{
		shortname: fullname,
	})
	return
}


func GetInstance(shortname string) ([]byte, error) {
	values, err := nameContainer.GetName(shortname)
	if err != nil {
		log.Printf("Failed to get instance[shortname:%s, err: %v]", shortname, err)
		return nil, err
	}
	return values, nil
}

func GetMaster(shortname string) ([]byte, error) {
	values, err := nameContainer.GetMaster(shortname)
	if err != nil {
		log.Printf("Failed to get master[shortname:%s, err: %v]", shortname, err)
		return nil, err
	}
	return values, nil
}

func GetAllInstance(shortname string) ([][]byte, error) {
	return nameContainer.GetNameBatch(shortname)
}
