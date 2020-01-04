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
	handler   Handler
}

func (c *remoteConnection) ID() string {
	return c.id
}

func (c *remoteConnection) SetHandler(handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = handler
	return nil
}

func (c *remoteConnection) Handler() Handler {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handler
}

func (c *remoteConnection) Publish(topic Topic, data []byte) error {
	return c.send(Publish{Topic: topic, Payload: data})
}

func (c *remoteConnection) Subscribe(topicFilter Topic, qos QoS) error {
	return c.send(Subscribe{TopicFilters: []TopicFilterQoS{{topicFilter, qos}}})
}

func (c *remoteConnection) Unsubscribe(topicFilter Topic) error {
	return c.send(Unsubscribe{TopicFilters: []Topic{topicFilter}})
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
		if c.handler != nil {
			c.handler.HandleMQTT(c, v.Topic, v.Payload)
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
