package mqtt

import "sync"

var _ Handler = (*Router)(nil)

func NewRouter() *Router {
	return &Router{
		handlers: map[Topic]Handler{},
	}
}

type Router struct {
	mu       sync.RWMutex
	handlers map[Topic]Handler
}

func (r *Router) On(t Topic, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[t] = h
}

func (r *Router) Off(t Topic) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, t)
}

func (r *Router) HandleMQTT(c Connection, m Message) {
	r.mu.RLock()
	defer r.mu.RLock()
	for f, h := range r.handlers {
		if f.Accept(m.Topic) {
			h.HandleMQTT(c, m)
		}
	}
}
