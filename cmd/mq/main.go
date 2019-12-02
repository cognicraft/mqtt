package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/cognicraft/mqtt"
)

var version = "0.1"

func main() {
	bind := flag.String("bind", ":1883", "The bind address.")
	v := flag.Bool("version", false, "Version")
	flag.Parse()

	if *v {
		fmt.Printf("%s\n", version)
		os.Exit(0)
	}

	s := mqtt.NewServer(*bind)
	log.Fatal(s.ListenAndServe())
}
