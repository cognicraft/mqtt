package mqtt

import (
	"net"
	"sync"
)

type Connection interface {
	ID() string
	OnMessage(callback func(topic string, data []byte))
	Subscribe(topicFilter string, qos QoS) error
	Unsubscribe(topicFilter string) error
	Publish(topic string, data []byte) error
	Close() error
}

type connection struct {
	queue     *Queue
	id        string
	mu        sync.RWMutex
	subs      map[string]QoS
	onMessage func(string, []byte)
	conn      net.Conn
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) OnMessage(callback func(topic string, data []byte)) {
	c.mu.Lock()
	c.onMessage = callback
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
			if c.onMessage != nil {
				c.onMessage(topic, data)
			}
		}
	}
	return nil
}

func (c *connection) Close() error {
	return c.queue.disconnect(c.id)
}
