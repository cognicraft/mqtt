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

func Errorf(format string, args ...interface{}) {
	if !strings.HasSuffix(format, "\n") {
		format = format + "\n"
	}
	fmt.Printf(format, args...)
}

func NewServer(addr string) *Server {
	return &Server{
		Addr:   addr,
		Errorf: Errorf,

		connections: map[string]Connection{},
	}
}

type Server struct {
	Addr   string
	Errorf func(string, ...interface{})

	done        chan struct{}
	mu          sync.RWMutex
	connections map[string]Connection
}

func (s *Server) Connect(id string) (Connection, error) {
	return s.connect(id, nil)
}

func (s *Server) Connections() []Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var cs []Connection
	for _, c := range s.connections {
		cs = append(cs, c)
	}
	sort.Slice(cs, func(i, j int) bool {
		return cs[i].ID() < cs[j].ID()
	})
	return cs
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	s.done = make(chan struct{})
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		conn, err := ln.Accept()

		if err != nil {
			select {
			case <-s.done:
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
				s.Errorf("server/ListenAndServe: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) Close() error {
	close(s.done)
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	in := make(chan interface{})
	go func() {
		scanner := NewScanner(conn)
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
				if c, err = s.connect(m.ClientID, conn); err == nil {
					keepAlive = time.Second * time.Duration(m.KeepAlive+10)
					send(conn, ConnAck{})
					defer c.Close()
					fmt.Printf("[%s] Connect(keep-alive=%s)\n", c.ID(), keepAlive)
				} else {
					fmt.Printf("[%s] ERROR: %v\n", m.ClientID, err)
					return
				}
			case *Subscribe:
				for _, t := range m.TopicFilters {
					if err := c.Subscribe(t.TopicFilter, t.QoS); err == nil {
						fmt.Printf("[%s] Subscribe(filter=%s, qos=%d)\n", c.ID(), t.TopicFilter, t.QoS)
					}
				}
			case *Unsubscribe:
				for _, t := range m.TopicFilters {
					if err := c.Unsubscribe(t); err == nil {
						fmt.Printf("[%s] Unsubscribe(filter=%s)\n", c.ID(), t)
					}
				}
			case *Publish:
				s.Publish(m.Topic, m.Payload)
				aux := ""
				if len(m.Payload) > 0 && utf8.Valid(m.Payload) {
					aux = fmt.Sprintf(", payload=%q", string(m.Payload))
				}
				fmt.Printf("[%s] Publish(topic=%s%s)\n", c.ID(), m.Topic, aux)
			case *Disconnect:
				fmt.Printf("[%s] Disconnect()\n", c.ID())
				return
			case *PingReq:
				// used for keep alive
			default:
				fmt.Printf("[%s] unknown message: %#v\n", c.ID(), m)
			}
		}
	}
}

func (s *Server) Publish(topic string, data []byte) {
	for _, c := range s.connections {
		c.Publish(topic, data)
	}
}

func (s *Server) connect(id string, conn net.Conn) (Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.connections[id]; ok {
		return nil, fmt.Errorf("connection with id already exists")
	}
	c := &connection{
		server: s,
		id:     id,
		subs:   map[string]QoS{},
		conn:   conn,
	}
	s.connections[id] = c
	return c, nil
}

func (s *Server) disconnect(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connections, id)
	return nil
}
