package mqtt

import (
	"net"
	"sync"
)

type Connection interface {
	ID() string
	Subscribe(topicFilter Topic, qos QoS, handler Handler) error
	Unsubscribe(topicFilter Topic) error
	Publish(topic Topic, data []byte) error
	Close() error
}

type connection struct {
	queue *Queue
	id    string
	mu    sync.RWMutex
	subs  map[Topic]sub
	conn  net.Conn
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) Subscribe(filter Topic, qos QoS, handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[filter] = sub{filter: filter, qos: qos, handler: handler}
	return nil
}

func (c *connection) Unsubscribe(filter Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, filter)
	return nil
}

func (c *connection) Publish(topic Topic, data []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, sub := range c.subs {
		if sub.filter.Accept(topic) {
			if c.conn != nil {
				err := send(c.conn, Publish{Topic: topic, Payload: data})
				if err != nil {
					return err
				}
			}
			if sub.handler != nil {
				sub.handler.HandleMQTT(c, Message{Topic: topic, Payload: data})
			}
		}
	}
	return nil
}

func (c *connection) Close() error {
	return c.queue.disconnect(c.id)
}

type sub struct {
	filter  Topic
	qos     QoS
	handler Handler
}
