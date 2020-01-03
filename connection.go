package mqtt

import (
	"net"
	"sync"
)

type Connection interface {
	ID() string
	On(callback func(topic string, data []byte))
	Subscribe(topicFilter string, qos QoS) error
	Unsubscribe(topicFilter string) error
	Publish(topic string, data []byte) error
	Close() error
}

type connection struct {
	server *Server
	id     string
	mu     sync.RWMutex
	subs   map[string]QoS
	on     func(string, []byte)
	conn   net.Conn
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) On(callback func(topic string, data []byte)) {
	c.mu.Lock()
	c.on = callback
	c.mu.Unlock()
}

func (c *connection) Subscribe(topicFilter string, qos QoS) error {
	c.subs[topicFilter] = qos
	return nil
}

func (c *connection) Unsubscribe(topicFilter string) error {
	delete(c.subs, topicFilter)
	return nil
}

func (c *connection) Publish(topic string, data []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for t, _ := range c.subs {
		if Topic(t).Accept(Topic(topic)) {
			if c.conn != nil {
				err := send(c.conn, Publish{Topic: string(topic), Payload: data})
				if err != nil {
					return err
				}
			}
			if c.on != nil {
				c.on(topic, data)
			}
		}
	}
	return nil
}

func (c *connection) Close() error {
	return c.server.disconnect(c.id)
}
