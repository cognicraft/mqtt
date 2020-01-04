package mqtt

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

func Dial(clientID string, addr string) (Connection, error) {
	netConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &remoteConnection{
		id:        clientID,
		keepAlive: 30,
		conn:      netConn,
		subs:      map[Topic]sub{},
	}
	go c.run()
	return c, nil
}

type remoteConnection struct {
	done      chan struct{}
	closed    chan struct{}
	conn      io.ReadWriteCloser
	id        string
	keepAlive uint16
	lastSend  time.Time
	mu        sync.RWMutex
	subs      map[Topic]sub
}

func (c *remoteConnection) ID() string {
	return c.id
}

func (c *remoteConnection) Publish(topic Topic, data []byte) error {
	return c.send(Publish{Topic: topic, Payload: data})
}

func (c *remoteConnection) Subscribe(topicFilter Topic, qos QoS, handler Handler) error {
	err := c.send(Subscribe{TopicFilters: []TopicFilterQoS{{topicFilter, qos}}})
	if err == nil {
		c.mu.Lock()
		c.subs[topicFilter] = sub{filter: topicFilter, qos: qos, handler: handler}
		c.mu.Unlock()
	}
	return err
}

func (c *remoteConnection) Unsubscribe(topicFilter Topic) error {
	err := c.send(Unsubscribe{TopicFilters: []Topic{topicFilter}})
	if err == nil {
		c.mu.Lock()
		delete(c.subs, topicFilter)
		c.mu.Unlock()
	}
	return err
}

func (c *remoteConnection) Close() error {
	close(c.done)
	<-c.closed
	return nil
}

func (c *remoteConnection) run() {
	c.closed = make(chan struct{})
	c.done = make(chan struct{})
	c.send(Connect{ClientID: c.id, KeepAlive: c.keepAlive, CleanSession: true})
	go func() {
		scanner := NewScanner(c.conn)
		for scanner.Scan() {
			msg := scanner.Message()
			v, err := unmarshal(msg)
			if err != nil {
				fmt.Printf("error: %+v\n", err)
			}
			c.received(v)
		}
		fmt.Printf("error: %+v\n", scanner.Err())
	}()
	for {
		select {
		case <-time.After(time.Second):
			if time.Since(c.lastSend) > time.Second*time.Duration(c.keepAlive) {
				c.send(PingReq{})
			}
		case <-c.done:
			c.send(Disconnect{})
			close(c.closed)
			return
		}
	}
}

func (c *remoteConnection) send(v interface{}) error {
	if err := send(c.conn, v); err != nil {
		return err
	}
	c.lastSend = time.Now()
	// fmt.Printf("%s → %#v\n", time.Now().UTC().Format(time.RFC3339), v)
	return nil
}

func (c *remoteConnection) received(v interface{}) {
	// fmt.Printf("%s ← %#v\n", time.Now().UTC().Format(time.RFC3339), v)
	switch v := v.(type) {
	case *Publish:
		c.mu.RLock()
		for _, sub := range c.subs {
			if sub.filter.Accept(v.Topic) && sub.handler != nil {
				sub.handler.HandleMQTT(c, Message{Topic: v.Topic, Payload: v.Payload})
			}
		}
		c.mu.RUnlock()
	}
}

func send(w io.Writer, v interface{}) error {
	m, ok := v.(Marshaler)
	if !ok {
		return fmt.Errorf("unable to marshal: %v\n", v)
	}
	raw, err := m.MarshalMQTT()
	if err != nil {
		return err
	}
	_, err = w.Write(raw)
	return err
}
