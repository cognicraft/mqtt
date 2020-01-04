package mqtt

// A Handler responds to an MQTT message.
type Handler interface {
	HandleMQTT(c Connection, t Topic, d []byte)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as MQTT handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(c Connection, t Topic, d []byte)

// HandleMQTT calls f(c, t, d).
func (f HandlerFunc) HandleMQTT(c Connection, t Topic, d []byte) {
	f(c, t, d)
}
