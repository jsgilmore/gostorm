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

package json

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"log"
)

func newJsonInput(reader io.Reader) *jsonInput {
	return &jsonInput{
		reader:      bufio.NewReader(reader),
		tupleBuffer: list.New(),
	}
}

type jsonInput struct {
	reader      *bufio.Reader
	tupleBuffer *list.List
}

func (this *jsonInput) readData() (data []byte, err error) {
	// Read a single json record from the input file
	data, err = this.reader.ReadBytes('\n')
	if err != nil {
		return data, err
	}

	//Read the end delimiter
	_, err = this.reader.ReadBytes('\n')
	if err == io.EOF {
		panic("EOF received at end statement. Is there newline after every end statement (including the last one)?")
	} else if err != nil {
		panic(err)
	}

	// Remove the newline character
	data = bytes.TrimRight(data, "\n")
	return data, nil
}

// readBytes reads data from stdin into the struct provided.
func (this *jsonInput) ReadMsg(msg interface{}) (err error) {
	var data []byte
	// Read data from the tuple buffer
	if this.tupleBuffer.Len() > 0 {
		e := this.tupleBuffer.Front()
		data = this.tupleBuffer.Remove(e).([]byte)
		// if the tuple buffer is empty, read data from storm
	} else {
		data, err = this.readData()
		if err != nil {
			return err
		}
	}

	err = json.Unmarshal(data, msg)
	if err != nil {
		log.Printf("core json: Unmarshalling: %s", data)
		return err
	}
	return nil
}

func (this *jsonInput) ReadTaskIds() (taskIds []int32) {
	// Read a single json record from the input file
	data, err := this.readData()
	if err != nil {
		panic(err)
	}

	// If we didn't receive a json array, treat it as a tuple instead
	if data[0] != '[' {
		this.tupleBuffer.PushBack(data)
		return this.ReadTaskIds()
	}

	err = json.Unmarshal(data, &taskIds)
	if err != nil {
		panic(err)
	}
	return taskIds
}

func newJsonOutput(writer io.Writer) *jsonOutput {
	return &jsonOutput{
		writer: bufio.NewWriter(writer),
	}
}

type jsonOutput struct {
	writer *bufio.Writer
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *jsonOutput) SendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(this.writer, string(data))
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
}

func (this *jsonOutput) Flush() {
	this.writer.Flush()
}
