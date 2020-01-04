package mqtt

type Message struct {
	Topic   Topic
	Payload []byte
}

// A Handler responds to an MQTT message.
type Handler interface {
	HandleMQTT(c Connection, m Message)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as MQTT handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(c Connection, m Message)

// HandleMQTT calls f(c, t, d).
func (f HandlerFunc) HandleMQTT(c Connection, m Message) {
	f(c, m)
}
