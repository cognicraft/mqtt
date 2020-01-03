package mqtt

import (
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

func Discardf(format string, args ...interface{}) {
}

func Printf(format string, args ...interface{}) {
	if !strings.HasSuffix(format, "\n") {
		format = format + "\n"
	}
	fmt.Printf(format, args...)
}

func Loger(logf func(string, ...interface{})) func(q *Queue) {
	return func(q *Queue) {
		q.logf = logf
	}
}

func NewQueue(bind string, opts ...func(q *Queue)) *Queue {
	q := &Queue{
		bind:        bind,
		logf:        Discardf,
		connections: map[string]Connection{},
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

type Queue struct {
	bind string
	logf func(string, ...interface{})

	done        chan struct{}
	mu          sync.RWMutex
	connections map[string]Connection
}

func (q *Queue) Connect(id string) (Connection, error) {
	return q.connect(id, nil)
}

func (q *Queue) Connections() []Connection {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var cs []Connection
	for _, c := range q.connections {
		cs = append(cs, c)
	}
	sort.Slice(cs, func(i, j int) bool {
		return cs[i].ID() < cs[j].ID()
	})
	return cs
}

func (q *Queue) Publish(topic string, data []byte) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, c := range q.connections {
		c.Publish(topic, data)
	}
}

func (q *Queue) Close() error {
	close(q.done)
	return nil
}

func (q *Queue) ListenAndServe() error {
	ln, err := net.Listen("tcp", q.bind)
	if err != nil {
		return err
	}
	defer ln.Close()

	q.done = make(chan struct{})
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		conn, err := ln.Accept()

		if err != nil {
			select {
			case <-q.done:
				return nil
			default:
			}

			// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				q.logf("server/ListenAndServe: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		go q.handleConnection(conn)
	}
}

func (q *Queue) handleConnection(conn net.Conn) {
	defer conn.Close()
	in := make(chan interface{})
	go func() {
		scanner := NewScanner(conn)
		for scanner.Scan() {
			msg := scanner.Message()
			v, err := unmarshal(msg)
			if err != nil {
				q.logf("error: %+v\n", err)
				return
			}
			in <- v
		}
		switch scanner.Err() {
		case io.EOF:
		default:
			q.logf("error: %+v\n", scanner.Err())
		}
	}()
	keepAlive := time.Second * 15
	var c Connection
	var err error
	for {
		select {
		case <-time.After(keepAlive):
			return
		case m := <-in:
			switch m := m.(type) {
			case *Connect:
				if c, err = q.connect(m.ClientID, conn); err == nil {
					keepAlive = time.Second * time.Duration(m.KeepAlive+10)
					send(conn, ConnAck{})
					defer c.Close()
					q.logf("[%s] Connect(keep-alive=%s)\n", c.ID(), keepAlive)
				} else {
					q.logf("[%s] ERROR: %v\n", m.ClientID, err)
					return
				}
			case *Subscribe:
				for _, t := range m.TopicFilters {
					if err := c.Subscribe(t.TopicFilter, t.QoS); err == nil {
						q.logf("[%s] Subscribe(filter=%s, qos=%d)\n", c.ID(), t.TopicFilter, t.QoS)
					}
				}
			case *Unsubscribe:
				for _, t := range m.TopicFilters {
					if err := c.Unsubscribe(t); err == nil {
						q.logf("[%s] Unsubscribe(filter=%s)\n", c.ID(), t)
					}
				}
			case *Publish:
				q.Publish(m.Topic, m.Payload)
				aux := ""
				if len(m.Payload) > 0 && utf8.Valid(m.Payload) {
					aux = fmt.Sprintf(", payload=%q", string(m.Payload))
				}
				q.logf("[%s] Publish(topic=%s%s)\n", c.ID(), m.Topic, aux)
			case *Disconnect:
				q.logf("[%s] Disconnect()\n", c.ID())
				return
			case *PingReq:
				// used for keep alive
			default:
				q.logf("[%s] unknown message: %#v\n", c.ID(), m)
			}
		}
	}
}

func (q *Queue) connect(id string, conn net.Conn) (Connection, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.connections[id]; ok {
		return nil, fmt.Errorf("connection with id already exists")
	}
	c := &connection{
		queue: q,
		id:    id,
		subs:  map[string]QoS{},
		conn:  conn,
	}
	q.connections[id] = c
	return c, nil
}

func (q *Queue) disconnect(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.connections, id)
	return nil
}
