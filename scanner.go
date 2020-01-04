package mqtt

import (
	"bufio"
	"fmt"
	"io"
)

type Scanner struct {
	in  *bufio.Reader
	err error
	msg RawMessage
}

func NewScanner(in io.Reader) *Scanner {
	return &Scanner{
		in: bufio.NewReader(in),
	}
}

func (s *Scanner) Message() RawMessage {
	return s.msg
}

func (s *Scanner) Err() error {
	return s.err
}

func (s *Scanner) Scan() bool {
	buf := newBuffer(nil)
	cp, err := s.in.ReadByte()
	if err != nil {
		s.err = err
		return false
	}
	buf.WriteByte(cp)
	rl, encrl, err := s.readRemainingLength()
	if err != nil {
		s.err = err
		return false
	}
	buf.Write(encrl)
	body := make([]byte, rl)
	_, err = io.ReadFull(s.in, body)
	if err != nil {
		s.err = err
		return false
	}
	buf.Write(body)
	s.msg = RawMessage(buf.Bytes())
	return true
}

func (s *Scanner) readRemainingLength() (int, []byte, error) {
	buf := newBuffer(nil)
	multiplier := 1
	value := 0
	for {
		encoded, err := s.in.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		value += int(encoded&127) * multiplier
		if multiplier > 128*128*128 {
			return 0, nil, fmt.Errorf("maximum length is exceeded")
		}
		buf.WriteByte(encoded)
		multiplier *= 128
		if encoded&128 == 0 {
			return value, buf.Bytes(), nil
		}
	}
}
