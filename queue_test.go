package mqtt

import "testing"

func TestQueue(t *testing.T) {
	q := NewQueue(":1883")
	c1, err := q.Connect("a")
	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}
	if c1 == nil {
		t.Fatalf("expected a valid connection")
	}
	if len(q.Connections()) != 1 {
		t.Errorf("expected %q connections", 1)
	}
	c2, err := q.Connect("a")
	if err == nil {
		t.Errorf("expected an error")
	}
	if c2 != nil {
		t.Errorf("expected a nil connection")
	}
	if len(q.Connections()) != 1 {
		t.Errorf("expected %q connections", 1)
	}
	c1.Close()
}
