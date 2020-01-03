package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cognicraft/mqtt"
)

func main() {
	s := mqtt.NewServer(":1883")
	sess, err := s.Connect("$xds")
	if err != nil {
		log.Fatal(err)
	}
	sess.Subscribe("xds/#", 0)
	sess.On(logAll)
	NewCopyCat(s)
	log.Fatal(s.ListenAndServe())
}

func logAll(topic string, data []byte) {
	fmt.Printf("%s %s: %s\n", time.Now().UTC().Format(time.RFC3339), topic, string(data))
}

func NewCopyCat(server *mqtt.Server) *CopyCat {
	c := &CopyCat{
		server: server,
	}
	s, _ := server.Connect("$cat")
	s.Subscribe("hbd/+/temp", 0)
	s.On(c.Copy)
	return c
}

type CopyCat struct {
	server *mqtt.Server
	con    mqtt.Connection
}

func (c *CopyCat) Copy(topic string, data []byte) {
	ps := strings.Split(topic, "/")
	sid := ps[1]
	ps = ps[2:]
	pos := "unknown"
	switch sid {
	case "1":
		pos = "left-outer-bearing"
	case "2":
		pos = "left-wheel"
	case "3":
		pos = "left-disk"
	case "4":
		pos = "left-engine"
	case "5":
		pos = "right-engine"
	case "6":
		pos = "right-disk"
	case "7":
		pos = "right-wheel"
	case "8":
		pos = "right-outer-bearing"
	}
	suffix := strings.Join(ps, "/")
	c.server.Publish("xds/1/"+pos+"/"+suffix, data)
}
