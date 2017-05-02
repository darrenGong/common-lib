package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"github.com/pkg/errors"
	"fmt"
	"math/rand"
)

type ZKConn struct {
	zkConn	*zk.Conn
}

/*
 @path Absolute path to the upper level
*/
func (c *ZKConn) GetNode(path string) ([]byte, error) {
	if _, err := c.valid(path); err != nil {
		return nil, err
	}

	children, err := c.listChildren(path)
	if err != nil {
		return nil, err
	}
	absoluteNode := strings.Join([]string{path,
		children[rand.Intn(len(children))]}, "/")
	value, _, err := c.zkConn.Get(absoluteNode)
	if err != nil {
		return nil, err
	}

	return value, nil
}

/**
	@path Absolute path
*/
func (c *ZKConn) GetNodeAbsolutePath(absolutePath string) ([]byte, error) {
	if _, err := c.valid(absolutePath); err != nil {
		return nil, err
	}

	value, _, err := c.zkConn.Get(absolutePath)
	if err != nil {
		return nil, err
	}

	return value, nil
}

/*
	@path absolute path
*/
func (c *ZKConn) SetNode(path string, values []byte) (string, error) {
	if _, err := c.valid(path); err != nil {
		return "", err
	}

	bValid, stat, err := c.zkConn.Exists(path)
	if err != nil {
		fmt.Errorf("Failed to check valid node path[%s]", path)
		return path, err
	}

	if ! bValid {
		return path, fmt.Errorf("Invalid path[%s]", path)
	}

	if _, err := c.zkConn.Set(path, values, stat.Version); err != nil {
		fmt.Errorf("Failed to set path[%s]", path)
		return path, err
	}

	return path, nil
}

func (c *ZKConn) CreateNode(path string, values []byte) (string, error) {
	if _, err := c.valid(path); err != nil {
		return "", err
	}
	var nodePath, reallyPath string
	paths := strings.Split(path, "/")
	for index, node := range paths[1:] {
		nodePath = strings.Join([]string{nodePath, node}, "/")
		bValid, _, err := c.zkConn.Exists(nodePath)
		if err != nil {
			fmt.Errorf("Failed to check valid node path[%s]", nodePath)
			return "", err
		}

		if bValid {
			continue
		}

		flags := int32(0)
		value := []byte(nil)
		if index == len(paths) - 2 {
			flags = zk.FlagEphemeral
			value = values
		}

		reallyPath, err = c.zkConn.Create(nodePath, value, flags, zk.WorldACL(zk.PermAll))
		if err != nil {
			fmt.Errorf("Failed to create node path[%s]", nodePath)
			return "", err
		}

	}

	return reallyPath, nil
}

func (c *ZKConn) listChildren(path string) ([]string, error) {
	children, _, err := c.zkConn.Children(path)
	if err != nil {
		return nil, err
	}

	return children, nil
}

func (c *ZKConn) ListChildrenWatch(path string) ([]string, <-chan zk.Event, error) {
	children, _, ev, err := c.zkConn.ChildrenW(path)
	if err != nil {
		return nil, nil, err
	}

	return children, ev, nil
}

func (c *ZKConn) reconnect() error {
	server, err := getServerByZKConn(c)
	if err != nil {
		return err
	}

	c, err = GetZKConn(server)
	if err != nil {
		return err
	}

	return nil
}

func (c *ZKConn) valid(path string) (bool, error) {
	if "" == path {
		return false, errors.New("Param[path] is empty")
	}

	if c.zkConn.State() == zk.StateDisconnected {
		fmt.Errorf("Invalid zookeeper connect[conn: %v], so reconnect[path:%s]",
			c.zkConn, path)
		if err := c.reconnect(); err != nil {
			return false, err
		}
	}

	return true, nil
}