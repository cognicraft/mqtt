package mqtt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Marshaler interface {
	MarshalMQTT() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalMQTT([]byte) error
}

func Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(Marshaler); ok {
		return m.MarshalMQTT()
	}
	return nil, fmt.Errorf("unable to marshal: %v", v)
}

func Unmarshal(data []byte, v interface{}) error {
	if u, ok := v.(Unmarshaler); ok {
		return u.UnmarshalMQTT(data)
	}
	return fmt.Errorf("unable to unmarshal into: %v", v)
}

func unmarshal(m Message) (interface{}, error) {
	var un Unmarshaler
	switch m.Type() {
	case CONNECT:
		un = &Connect{}
	case CONNACK:
		un = &ConnAck{}
	case PUBLISH:
		un = &Publish{}
	case PUBACK:
		un = &PubAck{}
	case PUBREC:
		un = &PubRec{}
	case PUBREL:
		un = &PubRel{}
	case PUBCOMP:
		un = &PubComp{}
	case SUBSCRIBE:
		un = &Subscribe{}
	case SUBACK:
		un = &SubAck{}
	case UNSUBSCRIBE:
		un = &Unsubscribe{}
	case UNSUBACK:
		un = &UnsubAck{}
	case PINGREQ:
		un = &PingReq{}
	case PINGRESP:
		un = &PingResp{}
	case DISCONNECT:
		un = &Disconnect{}
	default:
		return nil, fmt.Errorf("unknown type: %v", m.Type())
	}
	err := un.UnmarshalMQTT(m)
	return un, err
}

type Message []byte

func (m Message) Type() ControlPacketType {
	b := m[0]
	cp := b >> 4
	return ControlPacketType(cp)
}

func (m Message) Flags() byte {
	b := m[0]
	return b & 0x0F
}

func (m Message) Body() []byte {
	rl, err := decodeRemainingLength(m[1:])
	if err != nil {
		// invalid remaining length
		fmt.Printf("invalid remainig length: %s", err)
		return nil
	}
	rld, _ := remainingLengthDigits(rl)
	body := m[1+rld:]
	if len(body) != rl {
		// invalid body
		return nil
	}
	return body
}

func (m Message) String() string {
	return fmt.Sprintf("%s %x %x", m.Type(), m.Flags(), m.Body())
}

func TypeFlags(typ ControlPacketType, flags byte) byte {
	tf := byte(typ << 4)
	tf |= flags
	return tf
}

type ControlPacketType byte

const (
	_ ControlPacketType = iota // ignore 0: Forbidden
	CONNECT
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
	_ // ignore 15: Forbidden
)

func (t ControlPacketType) String() string {
	switch t {
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	default:
		return "unknown"
	}
}

const (
	MaxRemainingLength = 268435455
)

func remainingLengthDigits(x int) (int, error) {
	if x < 0 {
		return 0, fmt.Errorf("remaining length may not be negative: %d", x)
	}
	if x < 128 {
		return 1, nil
	}
	if x < 16384 {
		return 2, nil
	}
	if x < 2097152 {
		return 3, nil
	}
	if x < 268435456 {
		return 4, nil
	}
	return 0, fmt.Errorf("remaining length may not exceed %d: %d", MaxRemainingLength, x)
}

func encodeRemainingLength(x int) ([]byte, error) {
	res := []byte{}
	for {
		encoded := byte(x % 128)
		x = x / 128
		if x > 0 {
			encoded = encoded | 128
		}
		res = append(res, encoded)
		if x == 0 {
			return res, nil
		}
	}
}

func decodeRemainingLength(bs []byte) (int, error) {
	multiplier := 1
	value := 0
	i := 0
	for {
		encoded := bs[i]
		value += int(encoded&127) * multiplier
		if multiplier > 128*128*128 {
			return 0, fmt.Errorf("maximum length is exceeded")
		}
		multiplier *= 128
		if encoded&128 == 0 {
			return value, nil
		}
		i++
	}
}

func encodeUint16(v uint16) []byte {
	res := make([]byte, 2)
	binary.BigEndian.PutUint16(res, v)
	return res
}

func encodeString(v string) []byte {
	l := len(v)
	res := make([]byte, 2+l)
	binary.BigEndian.PutUint16(res, uint16(l))
	copy(res[2:], []byte(v))
	return res
}

func decodeString(r io.Reader) string {
	l := uint16(0)
	binary.Read(r, binary.BigEndian, &l)
	v := make([]byte, l)
	r.Read(v)
	return string(v)
}

type Connect struct {
	ClientID     string
	UserName     *string
	Password     *string
	Will         *Will
	CleanSession bool
	KeepAlive    uint16
}

type Will struct {
	Topic   string
	Payload []byte
	QoS     QoS
	Retain  bool
}

func (m Connect) MarshalMQTT() ([]byte, error) {
	body := newBuffer(nil)
	body.WriteMQTTString("MQTT")
	body.WriteByte(0x04)

	flags := byte(0)
	if m.UserName != nil {
		flags |= maskConnectUserName
	}
	if m.Password != nil {
		flags |= maskConnectPassword
	}
	if m.Will != nil {
		flags |= maskConnectWill
		if m.Will.Retain {
			flags |= maskConnectWillRetain
		}
		flags |= byte(m.Will.QoS) << 3
	}
	if m.CleanSession {
		flags |= maskConnectCleanSession
	}
	body.WriteByte(flags)
	body.WriteUint16(m.KeepAlive)
	body.WriteMQTTString(m.ClientID)
	if m.Will != nil {
		body.WriteMQTTString(m.Will.Topic)
		body.WriteUint16(uint16(len(m.Will.Payload)))
		body.Write(m.Will.Payload)
	}
	if m.UserName != nil {
		body.WriteMQTTString(*m.UserName)
	}
	if m.Password != nil {
		body.WriteMQTTString(*m.Password)
	}

	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(CONNECT, 0))
	buf.WriteRemainingLength(body.Len())
	buf.Write(body.Bytes())
	return buf.Bytes(), nil
}

func (m *Connect) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != CONNECT {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	proto := body.ReadMQTTString()
	if proto != "MQTT" {
		return fmt.Errorf("unknown protocol: %s", proto)
	}
	v, _ := body.ReadByte()
	if v != 0x04 {
		return fmt.Errorf("unknown version: %d", v)
	}
	fields, _ := body.ReadByte()
	m.CleanSession = isSet(fields, maskConnectCleanSession)
	m.KeepAlive, _ = body.ReadUint16()
	m.ClientID = body.ReadMQTTString()
	if isSet(fields, maskConnectWill) {
		w := Will{}
		w.Topic = body.ReadMQTTString()
		l, _ := body.ReadUint16()
		w.Payload = make([]byte, l)
		body.Read(w.Payload)
		w.Retain = isSet(fields, maskConnectWillRetain)
		qos := (fields & maskConnectWillQoS) >> 3
		w.QoS = QoS(qos)
		m.Will = &w
	}
	if isSet(fields, maskConnectUserName) {
		user := body.ReadMQTTString()
		m.UserName = &user
	}
	if isSet(fields, maskConnectPassword) {
		pass := body.ReadMQTTString()
		m.Password = &pass
	}
	return nil
}

const (
	maskConnectUserName     byte = 1 << 7
	maskConnectPassword     byte = 1 << 6
	maskConnectWillRetain   byte = 1 << 5
	maskConnectWillQoS      byte = 3 << 3
	maskConnectWill         byte = 1 << 2
	maskConnectCleanSession byte = 1 << 1
	maskConnectReserved     byte = 1
)

func isSet(fields byte, mask byte) bool {
	return fields&mask == mask
}

type ConnAck struct {
	SessionPresent bool
	Code           ConnectReturnCode
}

const (
	maskConnackSessionPresent byte = 0x1
)

func (m ConnAck) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(CONNACK, 0))
	buf.WriteByte(0x02)

	flags := byte(0)
	if m.SessionPresent {
		flags |= maskConnackSessionPresent
	}
	buf.WriteByte(flags)

	buf.WriteByte(byte(m.Code))
	return buf.Bytes(), nil
}

func (m *ConnAck) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != CONNACK {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	bs := raw.Body()
	if len(bs) != 2 {
		return fmt.Errorf("invalid length: %d", len(bs))
	}
	m.SessionPresent = isSet(bs[0], maskConnackSessionPresent)
	m.Code = ConnectReturnCode(bs[1])
	return nil
}

type ConnectReturnCode byte

const (
	ConnectionAccepted ConnectReturnCode = iota
	ConnectionRefusedUnacceptableProtocolVersion
	ConnectionRefusedIdenitifierRejected
	ConnectionRefusedServerUnavailable
	ConnectionRefusedBadUserNameOrPassword
	ConnectionRefusedNotAuthorized
)

type Publish struct {
	ID      uint16
	Topic   string
	Payload []byte
	DUP     bool
	QoS     QoS
	Retain  bool
}

func (m Publish) MarshalMQTT() ([]byte, error) {
	flags := byte(0)
	if m.DUP {
		flags |= 0x08
	}
	flags |= (byte(m.QoS) << 1)
	if m.Retain {
		flags |= 0x01
	}

	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PUBLISH, flags))

	body := newBuffer(nil)
	body.WriteMQTTString(m.Topic)
	if m.QoS == 1 || m.QoS == 2 {
		body.WriteUint16(m.ID)
	}
	body.Write(m.Payload)

	buf.WriteRemainingLength(body.Len())
	buf.Write(body.Bytes())
	return buf.Bytes(), nil
}

func (m *Publish) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PUBLISH {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	flags := raw.Flags()
	m.DUP = (flags&0x08 != 0)
	m.QoS = QoS((flags >> 1) & 0x03)
	m.Retain = flags&0x01 == 0x01
	body := newBuffer(raw.Body())
	m.Topic = body.ReadMQTTString()
	if m.QoS == 1 || m.QoS == 2 {
		m.ID, _ = body.ReadUint16()
	}
	m.Payload = body.Bytes()
	return nil
}

type QoS byte

type PubAck struct {
	ID uint16
}

func (m PubAck) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PUBACK, 0))
	buf.WriteByte(0x02)
	buf.WriteUint16(m.ID)
	return buf.Bytes(), nil
}

func (m *PubAck) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PUBACK {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	if body.Len() != 2 {
		return fmt.Errorf("invalid length: %d", body.Len())
	}
	var err error
	m.ID, err = body.ReadUint16()
	return err
}

type PubRec struct {
	ID uint16
}

func (m PubRec) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PUBREC, 0))
	buf.WriteByte(0x02)
	buf.WriteUint16(m.ID)
	return buf.Bytes(), nil
}

func (m *PubRec) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PUBREC {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	if body.Len() != 2 {
		return fmt.Errorf("invalid length: %d", body.Len())
	}
	var err error
	m.ID, err = body.ReadUint16()
	return err
}

type PubRel struct {
	ID uint16
}

func (m PubRel) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PUBREL, 0x02))
	buf.WriteByte(0x02)
	buf.WriteUint16(m.ID)
	return buf.Bytes(), nil
}

func (m *PubRel) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PUBREL {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	if body.Len() != 2 {
		return fmt.Errorf("invalid length: %d", body.Len())
	}
	var err error
	m.ID, err = body.ReadUint16()
	return err
}

type PubComp struct {
	ID uint16
}

func (m PubComp) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PUBCOMP, 0))
	buf.WriteByte(0x02)
	buf.WriteUint16(m.ID)
	return buf.Bytes(), nil
}

func (m *PubComp) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PUBCOMP {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	if body.Len() != 2 {
		return fmt.Errorf("invalid length: %d", body.Len())
	}
	var err error
	m.ID, err = body.ReadUint16()
	return err
}

type Subscribe struct {
	ID           uint16
	TopicFilters []TopicFilterQoS
}

func (m Subscribe) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(SUBSCRIBE, 0x02))

	body := newBuffer(nil)
	body.WriteUint16(m.ID)
	for _, t := range m.TopicFilters {
		body.WriteMQTTString(t.TopicFilter)
		body.WriteByte(byte(t.QoS))
	}

	buf.WriteRemainingLength(body.Len())
	buf.Write(body.Bytes())
	return buf.Bytes(), nil
}

func (m *Subscribe) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != SUBSCRIBE {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	m.ID, _ = body.ReadUint16()
	for {
		t := TopicFilterQoS{}
		t.TopicFilter = body.ReadMQTTString()
		qos, _ := body.ReadByte()
		t.QoS = QoS(qos)
		m.TopicFilters = append(m.TopicFilters, t)
		if body.Len() == 0 {
			break
		}
	}
	return nil
}

type TopicFilterQoS struct {
	TopicFilter string
	QoS         QoS
}

type SubAck struct {
	ID          uint16
	ReturnCodes []ReturnCode
}

func (m SubAck) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(SUBACK, 0))

	body := newBuffer(nil)
	body.WriteUint16(m.ID)
	for _, rc := range m.ReturnCodes {
		body.WriteByte(byte(rc))
	}

	buf.WriteRemainingLength(body.Len())
	buf.Write(body.Bytes())
	return buf.Bytes(), nil
}

func (m *SubAck) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != SUBACK {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	var err error
	m.ID, err = body.ReadUint16()
	for {
		rc, _ := body.ReadByte()
		m.ReturnCodes = append(m.ReturnCodes, ReturnCode(rc))
		if body.Len() == 0 {
			break
		}
	}
	return err
}

type ReturnCode byte

const (
	ReturnCodeSuccessQoS0 ReturnCode = 0x00
	ReturnCodeSuccessQoS1 ReturnCode = 0x01
	ReturnCodeSuccessQoS2 ReturnCode = 0x02
	ReturnCodeFailure     ReturnCode = 0x80
)

type Unsubscribe struct {
	ID           uint16
	TopicFilters []string
}

func (m Unsubscribe) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(UNSUBSCRIBE, 0x02))

	body := newBuffer(nil)
	body.WriteUint16(m.ID)
	for _, t := range m.TopicFilters {
		body.WriteMQTTString(t)
	}

	buf.WriteRemainingLength(body.Len())
	buf.Write(body.Bytes())
	return buf.Bytes(), nil
}

func (m *Unsubscribe) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != UNSUBSCRIBE {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	m.ID, _ = body.ReadUint16()
	for {
		t := body.ReadMQTTString()
		m.TopicFilters = append(m.TopicFilters, t)
		if body.Len() == 0 {
			break
		}
	}
	return nil
}

type UnsubAck struct {
	ID uint16
}

func (m UnsubAck) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(UNSUBACK, 0))
	buf.WriteByte(0x02)
	buf.WriteUint16(m.ID)
	return buf.Bytes(), nil
}

func (m *UnsubAck) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != UNSUBACK {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	body := newBuffer(raw.Body())
	if body.Len() != 2 {
		return fmt.Errorf("invalid length: %d", body.Len())
	}
	var err error
	m.ID, err = body.ReadUint16()
	return err
}

type PingReq struct {
}

func (m PingReq) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PINGREQ, 0))
	buf.WriteByte(0)
	return buf.Bytes(), nil
}

func (m *PingReq) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PINGREQ {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	return nil
}

type PingResp struct {
}

func (m PingResp) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(PINGRESP, 0))
	buf.WriteByte(0)
	return buf.Bytes(), nil
}

func (m *PingResp) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != PINGRESP {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	return nil
}

type Disconnect struct {
}

func (m Disconnect) MarshalMQTT() ([]byte, error) {
	buf := newBuffer(nil)
	buf.WriteByte(TypeFlags(DISCONNECT, 0))
	buf.WriteByte(0)
	return buf.Bytes(), nil
}

func (m *Disconnect) UnmarshalMQTT(data []byte) error {
	raw := Message(data)
	if raw.Type() != DISCONNECT {
		return fmt.Errorf("invalid control packet type: %s", raw.Type())
	}
	return nil
}

func newBuffer(buf []byte) *buffer {
	return &buffer{
		Buffer: bytes.NewBuffer(buf),
	}
}

type buffer struct {
	*bytes.Buffer
}

func (b *buffer) WriteMQTTString(s string) (int, error) {
	return b.Buffer.Write(encodeString(s))
}

func (b *buffer) ReadMQTTString() string {
	return decodeString(b)
}

func (b *buffer) WriteUint16(v uint16) error {
	return binary.Write(b, binary.BigEndian, v)
}

func (b *buffer) ReadUint16() (uint16, error) {
	var v uint16
	err := binary.Read(b, binary.BigEndian, &v)
	return v, err
}

func (b *buffer) WriteRemainingLength(v int) (int, error) {
	rl, err := encodeRemainingLength(v)
	if err != nil {
		return 0, err
	}
	return b.Write(rl)
}
