package mqtt

import "testing"

func TestServer(t *testing.T) {
	s := NewServer(":1883")
	c1, err := s.Connect("a")
	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}
	if c1 == nil {
		t.Fatalf("expected a valid connection")
	}
	if len(s.Connections()) != 1 {
		t.Errorf("expected %q connections", 1)
	}
	c2, err := s.Connect("a")
	if err == nil {
		t.Errorf("expected an error")
	}
	if c2 != nil {
		t.Errorf("expected a nil connection")
	}
	if len(s.Connections()) != 1 {
		t.Errorf("expected %q connections", 1)
	}
	c1.Close()
}
