//   Copyright 2013 Vastech SA (PTY) LTD
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main

import (
	"fmt"
	"github.com/jsgilmore/gostorm"
	stormjson "github.com/jsgilmore/gostorm/encoding/json"
	"log"
	"os"
	"os/signal"
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
		boltConn.Emit([]string{id}, "", word)
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

	input := stormjson.NewJsonObjectInput(os.Stdin)
	output := stormjson.NewJsonObjectOutput(os.Stdout)
	boltConn := gostorm.NewBoltConn(input, output)
	boltConn.Initialise()

	for {
		var sentence string
		// We have to read Raw here, since the spout is not json encoding the tuple contents
		meta, err := boltConn.ReadTuple(sentence)
		if err != nil {
			panic(err)
		}
		emitWords(sentence, meta.Id, boltConn)
		boltConn.SendAck(meta.Id)
	}
}
