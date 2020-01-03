package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/cognicraft/mqtt"
)

func main() {
	c, err := mqtt.Dial("client", "127.0.0.1:1883")
	if err != nil {
		log.Fatal(err)
	}
	c.On(on)
	time.Sleep(time.Millisecond)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case s := <-signals:
			switch s {
			case os.Interrupt:
				c.Close()
				return
			}
		default:
			for i := 1; i <= 8; i++ {
				t := float64(rand.Intn(150) + 20)
				publihTemperature(c, i, t)
			}
		}
	}
}

func publihTemperature(c *mqtt.Conn, sid int, t float64) {
	prefix := fmt.Sprintf("hbd/%d", sid)
	c.Publish(prefix+"/temp", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/0", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/1", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/2", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/3", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/4", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/5", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/6", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/7", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/8", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/9", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/10", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/11", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/12", []byte(fmt.Sprintf("%.0f", t)))
	c.Publish(prefix+"/chan/13", []byte(fmt.Sprintf("%.0f", t)))
}

func on(topic string, data []byte) {
	fmt.Printf("on: %s - %s\n", topic, string(data))
}
