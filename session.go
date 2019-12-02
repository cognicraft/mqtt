package mqtt

import (
	"net"
	"sync"
)

type Session interface {
	Subscribe(topicFilter string, qos QoS) error
	Unsubscribe(topicFilter string) error
	Publish(topic string, data []byte) error
	Close() error
}

func newRemoteSession(server *Server, id string, conn net.Conn) *RemoteSession {
	return &RemoteSession{
		server: server,
		id:     id,
		subs:   map[string]QoS{},
		conn:   conn,
	}
}

type RemoteSession struct {
	server *Server
	id     string
	mu     sync.RWMutex
	subs   map[string]QoS
	conn   net.Conn
}

func (s *RemoteSession) Subscribe(topicFilter string, qos QoS) error {
	s.subs[topicFilter] = qos
	return nil
}

func (s *RemoteSession) Unsubscribe(topicFilter string) error {
	delete(s.subs, topicFilter)
	return nil
}

func (s *RemoteSession) Publish(topic string, data []byte) error {
	for t, _ := range s.subs {
		if Topic(t).Accept(Topic(topic)) {
			return send(s.conn, Publish{Topic: string(topic), Payload: data})
		}
	}
	return nil
}

func (s *RemoteSession) Close() error {
	return nil
}

func newLocalSession(server *Server, id string) *LocalSession {
	return &LocalSession{
		server: server,
		id:     id,
		subs:   map[string]QoS{},
	}
}

type LocalSession struct {
	server     *Server
	id         string
	mu         sync.RWMutex
	subs       map[string]QoS
	onReceived func(string, []byte)
}

func (s *LocalSession) OnReceived(callback func(topic string, data []byte)) {
	s.mu.Lock()
	s.onReceived = callback
	s.mu.Unlock()
}

func (s *LocalSession) Subscribe(topicFilter string, qos QoS) error {
	s.subs[topicFilter] = qos
	return nil
}

func (s *LocalSession) Unsubscribe(topicFilter string) error {
	delete(s.subs, topicFilter)
	return nil
}

func (s *LocalSession) Publish(topic string, data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.onReceived != nil {
		for t, _ := range s.subs {
			if Topic(t).Accept(Topic(topic)) {
				s.onReceived(topic, data)
				return nil
			}
		}
	}
	return nil
}

func (s *LocalSession) Close() error {
	return nil
}
