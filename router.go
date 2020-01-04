package mqtt

import (
	"sort"
	"sync"
)

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

func (r *Router) On(f Topic, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[f] = h
}

func (r *Router) Off(f Topic) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, f)
}

func (r *Router) Filters() []Topic {
	var fs []Topic
	for f := range r.handlers {
		fs = append(fs, f)
	}
	sort.Slice(fs, func(i, j int) bool {
		return fs[i] < fs[j]
	})
	return fs
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
