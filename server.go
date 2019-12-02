package mqtt

import (
	"fmt"
	"net"
	"strings"
	"time"
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

		sessions: map[string]Session{},
	}
}

type Server struct {
	Addr   string
	Errorf func(string, ...interface{})

	done     chan struct{}
	sessions map[string]Session
}

func (s *Server) Connect(id string) (*LocalSession, error) {
	sess := newLocalSession(s, id)
	s.sessions[id] = sess
	return sess, nil
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
		fmt.Printf("error: %+v\n", scanner.Err())
	}()
	keepAlive := time.Second * 15
	var sess *RemoteSession
	for {
		select {
		case <-time.After(keepAlive):
			return
		case m := <-in:
			switch m := m.(type) {
			case *Connect:
				sess = newRemoteSession(s, m.ClientID, conn)
				s.sessions[sess.id] = sess
				keepAlive = time.Second*time.Duration(m.KeepAlive) + 10
				send(conn, ConnAck{})
			case *Subscribe:
				for _, t := range m.TopicFilters {
					sess.Subscribe(t.TopicFilter, t.QoS)
				}
			case *Unsubscribe:
				for _, t := range m.TopicFilters {
					sess.Unsubscribe(t)
				}
			case *Publish:
				s.Publish(m.Topic, m.Payload)
			default:
				fmt.Printf("not handled: %#v\n", m)
			}
		}
	}
}

func (s *Server) Publish(topic string, data []byte) {
	for _, sess := range s.sessions {
		sess.Publish(topic, data)
	}
}
