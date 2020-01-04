package mqtt

import (
	"net"
	"sync"
)

type Connection interface {
	ID() string
	SetHandler(h Handler) error
	Handler() Handler
	Subscribe(topicFilter Topic, qos QoS) error
	Unsubscribe(topicFilter Topic) error
	Publish(topic Topic, data []byte) error
	Close() error
}

type connection struct {
	queue   *Queue
	id      string
	mu      sync.RWMutex
	subs    map[Topic]QoS
	handler Handler
	conn    net.Conn
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) SetHandler(handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = handler
	return nil
}

func (c *connection) Handler() Handler {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handler
}

func (c *connection) Subscribe(topicFilter Topic, qos QoS) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[topicFilter] = qos
	return nil
}

func (c *connection) Unsubscribe(topicFilter Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, topicFilter)
	return nil
}

func (c *connection) Publish(topic Topic, data []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for t, _ := range c.subs {
		if t.Accept(topic) {
			if c.conn != nil {
				err := send(c.conn, Publish{Topic: topic, Payload: data})
				if err != nil {
					return err
				}
			}
			if c.handler != nil {
				c.handler.HandleMQTT(c, Message{Topic: topic, Payload: data})
			}
		}
	}
	return nil
}

func (c *connection) Close() error {
	return c.queue.disconnect(c.id)
}
