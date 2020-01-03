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

	serverCommand := flag.NewFlagSet("server", flag.ExitOnError)
	serverBind := serverCommand.String("bind", ":1883", "the bind addresss for the server")
	serverCommand.Usage = func() {
		fmt.Println("usage: mq server [<options>]")
		serverCommand.PrintDefaults()
	}

	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)
	clientID := clientCommand.String("id", "", "the client id")
	clientDial := clientCommand.String("dial", "", "the server to dial")
	clientCommand.Usage = func() {
		fmt.Println("usage: mq client [<options>]")
		clientCommand.PrintDefaults()
	}

	if len(os.Args) == 1 {
		fmt.Println("usage: mq <command> [<args>]")
		fmt.Println("The most commonly used es commands are: ")
		fmt.Println("  version   Provides a version.")
		fmt.Println("  server    Provides a mqtt server.")
		fmt.Println("  client    Provides a mqtt client.")
		return
	}

	switch os.Args[1] {
	case versionCommand.Name():
		versionCommand.Parse(os.Args[2:])
	case serverCommand.Name():
		serverCommand.Parse(os.Args[2:])
	case clientCommand.Name():
		clientCommand.Parse(os.Args[2:])
	default:
		fmt.Printf("%q is not a valid command.\n", os.Args[1])
		os.Exit(2)
	}

	switch {
	case versionCommand.Parsed():
		doVersion()
	case serverCommand.Parsed():
		log.Fatal(doServer(*serverBind))
	case clientCommand.Parsed():
		log.Fatal(doClient(*clientID, *clientDial))
	}
}

func doVersion() error {
	fmt.Println(version)
	return nil
}

func doServer(bind string) error {
	s := mqtt.NewServer(bind)
	return s.ListenAndServe()
}

func doClient(clientID string, addr string) error {
	c, err := mqtt.Dial(clientID, addr)
	if err != nil {
		return err
	}
	defer c.Close()

	prompt := func() {
		fmt.Printf("mq ← ")
	}

	c.On(func(topic string, data []byte) {
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
		case "sub":
			if err := c.Subscribe(ps[1]); err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
		case "unsub":
			if err := c.Unsubscribe(ps[1]); err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
		case "pub":
			if err := c.Publish(ps[1], []byte(strings.Join(ps[2:], " "))); err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
		case "close", "exit":
			return nil
		default:
			fmt.Printf("ERROR: %v\n", fmt.Errorf("unrecognized command: %s", ps[0]))
		}
		prompt()
	}
	return nil
}
