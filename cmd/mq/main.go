package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/cognicraft/mqtt"
)

var version = "0.1"

func main() {
	versionCommand := flag.NewFlagSet("version", flag.ExitOnError)
	versionCommand.Usage = func() {
		fmt.Println("usage: mq version")
		versionCommand.PrintDefaults()
	}

	queueCommand := flag.NewFlagSet("queue", flag.ExitOnError)
	queueCommand.Usage = func() {
		fmt.Println("usage: mq queue <bind>")
		queueCommand.PrintDefaults()
	}

	dialCommand := flag.NewFlagSet("dial", flag.ExitOnError)
	clientID := dialCommand.String("id", "client", "the client id")
	dialCommand.Usage = func() {
		fmt.Println("usage: mq dial [<options>] <addr>")
		dialCommand.PrintDefaults()
	}

	if len(os.Args) == 1 {
		fmt.Println("usage: mq <command> [<args>]")
		fmt.Println("The most commonly used es commands are: ")
		fmt.Println("  version  Provides a version.")
		fmt.Println("  queue    Provides a mqtt queue.")
		fmt.Println("  dial     Provides a mqtt connection.")
		return
	}

	switch os.Args[1] {
	case versionCommand.Name():
		versionCommand.Parse(os.Args[2:])
	case queueCommand.Name():
		queueCommand.Parse(os.Args[2:])
	case dialCommand.Name():
		dialCommand.Parse(os.Args[2:])
	default:
		fmt.Printf("%q is not a valid command.\n", os.Args[1])
		os.Exit(2)
	}

	switch {
	case versionCommand.Parsed():
		doVersion()
	case queueCommand.Parsed():
		log.Fatal(doQueue(queueCommand.Arg(0)))
	case dialCommand.Parsed():
		log.Fatal(doDial(*clientID, dialCommand.Arg(0)))
	}
}

func doVersion() error {
	fmt.Println(version)
	return nil
}

func doQueue(bind string) error {
	s := mqtt.NewQueue(bind, mqtt.Loger(mqtt.Printf))
	return s.ListenAndServe()
}

func doDial(clientID string, addr string) error {
	c, err := mqtt.Dial(clientID, addr)
	if err != nil {
		return err
	}
	defer c.Close()

	prompt := func() {
		fmt.Printf("mq ← ")
	}

	c.OnMessage(func(topic string, data []byte) {
		d := "[*]"
		if len(data) > 0 && utf8.Valid(data) {
			d = string(data)
		}

		fmt.Printf("\rmq → %s %s\n", topic, d)
		prompt()
	})

	prompt()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		ps := strings.Fields(line)
		if len(ps) == 0 {
			continue
		}
		switch ps[0] {
		case "subscribe", "sub", "s":
			if err := c.Subscribe(ps[1], 0); err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
		case "unsubscribe", "unsub", "u":
			if err := c.Unsubscribe(ps[1]); err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
		case "publish", "pub", "p":
			if err := c.Publish(ps[1], []byte(strings.Join(ps[2:], " "))); err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
		case "close", "exit", "q":
			return nil
		default:
			fmt.Printf("ERROR: %v\n", fmt.Errorf("unrecognized command: %s", ps[0]))
		}
		prompt()
	}
	return nil
}
