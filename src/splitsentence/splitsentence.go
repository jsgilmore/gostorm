package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"storm"
	"strings"
)

func handleSigTerm() {
	// Enable the capture of Ctrl-C, to cleanly close the application
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	sig := <-c
	log.Printf("Received %s signal, closing.", sig.String())
	os.Exit(1)
}

func emitWords(sentance, id string, boltConn gostorm.BoltConn) {
	words := strings.Split(sentance, " ")
	for _, word := range words {
		boltConn.Emit([]interface{}{word}, []string{id}, "")
	}
}

func main() {
	// Logging is done to an output file, since stdout and stderr are captured
	fo, err := os.Create(fmt.Sprintf("/Users/johngilmore/output%d.txt", os.Getpid()))
	//	fo, err := os.Create("/Users/johngilmore/output.txt")
	if err != nil {
		panic(err)
	}
	defer fo.Close()
	log.SetOutput(fo)
	//log.SetOutput(os.Stdout)

	// This section allows us to correctly log signals and system panics
	go handleSigTerm()
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("Recovered panic: %v", r)
		}
	}()

	boltConn := gostorm.NewBoltConn()
	boltConn.Initialise()

	for {
		tuple, eof := boltConn.ReadTuple()
		if eof {
			return
		}
		emitWords(tuple.Contents[0].(string), tuple.Id, boltConn)
		boltConn.SendAck(tuple.Id)
	}
}
