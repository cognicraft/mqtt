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
	onClose   func(Connection)
	subs      map[Topic]sub
}

func (c *remoteConnection) ID() string {
	return c.id
}

func (c *remoteConnection) Publish(topic Topic, data []byte) error {
	return c.send(Publish{Topic: topic, Payload: data})
}

func (c *remoteConnection) Subscribe(topicFilter Topic, qos QoS, handler Handler) error {
	if handler == nil {
		return fmt.Errorf("handler may not be nil")
	}
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

func (c *remoteConnection) OnClose(f func(c Connection)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onClose = f
}

func (c *remoteConnection) Close() error {
	close(c.done)
	<-c.closed
	return nil
}

func (c *remoteConnection) run() {
	defer func() {
		if c.onClose != nil {
			c.onClose(c)
		}
	}()
	c.closed = make(chan struct{})
	c.done = make(chan struct{})

	in := make(chan interface{})
	go func() {
		defer close(in)
		scanner := NewScanner(c.conn)
		for scanner.Scan() {
			msg := scanner.Message()
			v, err := unmarshal(msg)
			if err != nil {
				fmt.Printf("error: %+v\n", err)
				return
			}
			in <- v
		}
		switch scanner.Err() {
		case io.EOF:
		default:
			fmt.Printf("error: %+v\n", scanner.Err())
		}
	}()

	c.send(Connect{ClientID: c.id, KeepAlive: c.keepAlive, CleanSession: true})
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
		case m, ok := <-in:
			if !ok {
				return
			}
			switch m := m.(type) {
			case *Publish:
				c.publish(m.Topic, m.Payload)
			}
		}
	}
}

func (c *remoteConnection) publish(topic Topic, payload []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, sub := range c.subs {
		if sub.filter.Accept(topic) {
			sub.handler.HandleMQTT(c, Message{Topic: topic, Payload: payload})
		}
	}
	return nil
}

func (c *remoteConnection) send(v interface{}) error {
	if err := send(c.conn, v); err != nil {
		return err
	}
	c.lastSend = time.Now()
	return nil
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
