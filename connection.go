package mqtt

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

func Dial(clientID string, addr string) (*Conn, error) {
	netConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		id:        clientID,
		keepAlive: 30,
		conn:      netConn,
	}
	go c.run()
	return c, nil
}

type Conn struct {
	done       chan struct{}
	conn       io.ReadWriteCloser
	id         string
	keepAlive  uint16
	lastSend   time.Time
	mu         sync.RWMutex
	onReceived func(string, []byte)
}

func (c *Conn) OnReceived(callback func(topic string, data []byte)) {
	c.mu.Lock()
	c.onReceived = callback
	c.mu.Unlock()
}

func (c *Conn) Publish(topic string, data []byte) error {
	return c.send(Publish{Topic: topic, Payload: data})
}

func (c *Conn) Subscribe(topicFilter string) error {
	return c.send(Subscribe{TopicFilters: []TopicFilterQoS{{topicFilter, 0}}})
}
func (c *Conn) Unsubscribe(topicFilter string) error {
	return c.send(Unsubscribe{TopicFilters: []string{topicFilter}})
}

func (c *Conn) Close() error {
	close(c.done)
	return nil
}

func (c *Conn) run() {
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
			return
		}
	}
}

func (c *Conn) send(v interface{}) error {
	if err := send(c.conn, v); err != nil {
		return err
	}
	c.lastSend = time.Now()
	// fmt.Printf("%s → %#v\n", time.Now().UTC().Format(time.RFC3339), v)
	return nil
}

func (c *Conn) received(v interface{}) {
	// fmt.Printf("%s ← %#v\n", time.Now().UTC().Format(time.RFC3339), v)
	switch v := v.(type) {
	case *Publish:
		c.mu.RLock()
		if c.onReceived != nil {
			c.onReceived(v.Topic, v.Payload)
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
