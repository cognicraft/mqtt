package mqtt

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestType(t *testing.T) {
	tests := []struct {
		name string
		msg  RawMessage
		out  ControlPacketType
	}{
		{"connect", RawMessage{0x10, 0x00}, CONNECT},
		{"connack", RawMessage{0x20, 0x00}, CONNACK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.msg.Type()
			if test.out != got {
				t.Errorf("want: %v, got: %v", test.out, got)
			}
		})
	}

}

func TestFlags(t *testing.T) {
	tests := []struct {
		name string
		msg  RawMessage
		out  byte
	}{
		{"connect", RawMessage{0x10, 0x00}, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.msg.Flags()
			if test.out != got {
				t.Errorf("want: %v, got: %v", test.out, got)
			}
		})
	}
}

func TestDecodeRemainingLength(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		out  int
	}{
		{"0", []byte{0x00}, 0},
		{"127", []byte{0x7F}, 127},
		{"128", []byte{0x80, 0x01}, 128},
		{"16 383", []byte{0xFF, 0x7F}, 16383},
		{"16 384", []byte{0x80, 0x80, 0x01}, 16384},
		{"2 097 151", []byte{0xFF, 0xFF, 0x7F}, 2097151},
		{"2 097 152", []byte{0x80, 0x80, 0x80, 0x01}, 2097152},
		{"268 435 455", []byte{0xFF, 0xFF, 0xFF, 0x7F}, 268435455},
		{"268 435 456", []byte{0x80, 0x80, 0x80, 0x80, 0x01}, 0}, // to big
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := decodeRemainingLength(test.in)
			if test.out != got {
				t.Errorf("want: %v, got: %v", test.out, got)
			}
		})
	}
}

func TestEncodeRemainingLength(t *testing.T) {
	tests := []struct {
		name string
		in   int
		out  []byte
	}{
		{"0", 0, []byte{0x00}},
		{"127", 127, []byte{0x7F}},
		{"128", 128, []byte{0x80, 0x01}},
		{"16 383", 16383, []byte{0xFF, 0x7F}},
		{"16 384", 16384, []byte{0x80, 0x80, 0x01}},
		{"2 097 151", 2097151, []byte{0xFF, 0xFF, 0x7F}},
		{"2 097 152", 2097152, []byte{0x80, 0x80, 0x80, 0x01}},
		{"268 435 455", 268435455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := encodeRemainingLength(test.in)
			if !bytes.Equal(test.out, got) {
				t.Errorf("want: %v, got: %v", test.out, got)
			}
		})
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	user := "user"
	pass := "pass"
	tests := []struct {
		name        string
		marshaler   Marshaler
		unmarshaler Unmarshaler
	}{
		{"CONNECT", &Connect{ClientID: "sensor-a", CleanSession: true, Will: &Will{Topic: "a/b", Payload: []byte("Will"), QoS: 2, Retain: true}, UserName: &user, Password: &pass, KeepAlive: 10}, &Connect{}},
		{"CONNACK", &ConnAck{SessionPresent: true, Code: ConnectionRefusedBadUserNameOrPassword}, &ConnAck{}},
		{"PUBLISH", &Publish{DUP: true, QoS: 1, Retain: true, Topic: "a/b", ID: 10, Payload: []byte("Hello World")}, &Publish{}},
		{"PUBACK", &PubAck{ID: 10}, &PubAck{}},
		{"PUBREC", &PubRec{ID: 10}, &PubRec{}},
		{"PUBREL", &PubRel{ID: 10}, &PubRel{}},
		{"PUBCOMP", &PubComp{ID: 10}, &PubComp{}},
		{"SUBSCRIBE", &Subscribe{ID: 10, TopicFilters: []TopicFilterQoS{{"a/b", 1}, {"c/d", 2}}}, &Subscribe{}},
		{"SUBACK", &SubAck{ID: 10, ReturnCodes: []ReturnCode{ReturnCodeFailure, ReturnCodeSuccessQoS0, ReturnCodeSuccessQoS1, ReturnCodeSuccessQoS2}}, &SubAck{}},
		{"UNSUBSCRIBE", &Unsubscribe{ID: 10, TopicFilters: []Topic{"a/b", "c/d"}}, &Unsubscribe{}},
		{"UNSUBACK", &UnsubAck{ID: 10}, &UnsubAck{}},
		{"PINGREQ", &PingReq{}, &PingReq{}},
		{"PINGRESP", &PingResp{}, &PingResp{}},
		{"DISCONNECT", &Disconnect{}, &Disconnect{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.marshaler.MarshalMQTT()
			if err != nil {
				t.Errorf("marshal: %+v", err)
			}
			err = test.unmarshaler.UnmarshalMQTT(data)
			if err != nil {
				t.Errorf("unmarshal: %+v", err)
			}
			if !reflect.DeepEqual(test.marshaler, test.unmarshaler) {
				sbs, _ := json.Marshal(test.marshaler)
				tbs, _ := json.Marshal(test.unmarshaler)
				t.Errorf("not equal: %s → %x → %s", string(sbs), data, string(tbs))
			}
		})
	}
}
