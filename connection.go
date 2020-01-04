package mqtt

import (
	"fmt"
	"sync"
)

type Connection interface {
	ID() string
	Subscribe(topicFilter Topic, qos QoS, handler Handler) error
	Unsubscribe(topicFilter Topic) error
	Publish(topic Topic, data []byte) error
	Close() error
}

type localConnection struct {
	queue *Queue
	id    string
	mu    sync.RWMutex
	subs  map[Topic]sub
}

func (c *localConnection) ID() string {
	return c.id
}

func (c *localConnection) Subscribe(filter Topic, qos QoS, handler Handler) error {
	if handler == nil {
		return fmt.Errorf("handler may not be nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[filter] = sub{filter: filter, qos: qos, handler: handler}
	return nil
}

func (c *localConnection) Unsubscribe(filter Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, filter)
	return nil
}

func (c *localConnection) Publish(topic Topic, payload []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, sub := range c.subs {
		if sub.filter.Accept(topic) {
			sub.handler.HandleMQTT(c, Message{Topic: topic, Payload: payload})
		}
	}
	return nil
}

func (c *localConnection) Close() error {
	return c.queue.disconnect(c.id)
}

type sub struct {
	filter  Topic
	qos     QoS
	handler Handler
}
