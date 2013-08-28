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
	"github.com/jsgilmore/gostorm/encoding"
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
	data = bytes.Trim(data, "\n")
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
		log.Printf("Gostorm json encoding: Unmarshalling: %s", data)
		return err
	}
	return nil
}

func (this *jsonInput) ReadTaskIds() (taskIds []int) {
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

func NewJsonObjectInputFactory() encoding.InputFactory {
	return &jsonObjectInputFactory{}
}

type jsonObjectInputFactory struct{}

func (this *jsonObjectInputFactory) NewInput(reader io.Reader) encoding.Input {
	return NewJsonObjectInput(reader)
}

func NewJsonObjectInput(reader io.Reader) encoding.Input {
	return &jsonObjectInput{
		jsonInput: newJsonInput(reader),
	}
}

type jsonObjectInput struct {
	*jsonInput
}

// ConstructInput for object json returns a list of objects into which json objects should be unmarshalled
func (this *jsonObjectInput) ConstructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		contentList[i] = content
	}
	return contentList
}

func (this *jsonObjectInput) DecodeInput(contentList []interface{}, contentStructs ...interface{}) {

}

func NewJsonEncodedInputFactory() encoding.InputFactory {
	return &jsonEncodedInputFactory{}
}

type jsonEncodedInputFactory struct{}

func (this *jsonEncodedInputFactory) NewInput(reader io.Reader) encoding.Input {
	return NewJsonEncodedInput(reader)
}

func NewJsonEncodedInput(reader io.Reader) encoding.Input {
	return &jsonEncodedInput{
		jsonInput: newJsonInput(reader),
	}
}

type jsonEncodedInput struct {
	*jsonInput
}

// ConstructInput for encoded json returns a list of bytes into which marshalled json should be written
func (this *jsonEncodedInput) ConstructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i := 0; i < len(contents); i++ {
		contentList[i] = &[]byte{}
	}
	return contentList
}

func (this *jsonEncodedInput) DecodeInput(contentList []interface{}, contentStructs ...interface{}) {
	for i, content := range contentStructs {
		err := json.Unmarshal(*contentList[i].(*[]byte), content)
		if err != nil {
			panic(err)
		}
	}
}

func newJsonOutput(writer io.Writer) *jsonOutput {
	return &jsonOutput{
		writer: writer,
	}
}

type jsonOutput struct {
	writer io.Writer
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *jsonOutput) SendEncoded(msg string) {
	fmt.Fprintln(this.writer, msg)
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
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

func NewJsonEncodedOutputFactory() encoding.OutputFactory {
	return &jsonEncodedOutputFactory{}
}

type jsonEncodedOutputFactory struct{}

func (this *jsonEncodedOutputFactory) NewOutput(writer io.Writer) encoding.Output {
	return NewJsonEncodedOutput(writer)
}

func NewJsonEncodedOutput(writer io.Writer) encoding.Output {
	return &jsonEncodedOutput{
		jsonOutput: newJsonOutput(writer),
	}
}

type jsonEncodedOutput struct {
	*jsonOutput
}

func (this *jsonEncodedOutput) ConstructOutput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		encoded, err := json.Marshal(content)
		if err != nil {
			panic(err)
		}
		contentList[i] = &encoded
	}
	return contentList
}

func NewJsonObjectOutputFactory() encoding.OutputFactory {
	return &jsonObjectOutputFactory{}
}

type jsonObjectOutputFactory struct{}

func (this *jsonObjectOutputFactory) NewOutput(writer io.Writer) encoding.Output {
	return NewJsonObjectOutput(writer)
}

func NewJsonObjectOutput(writer io.Writer) encoding.Output {
	return &jsonObjectOutput{
		jsonOutput: newJsonOutput(writer),
	}
}

type jsonObjectOutput struct {
	*jsonOutput
}

func (this *jsonObjectOutput) ConstructOutput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		contentList[i] = content
	}
	return contentList
}

func init() {
	encoding.RegisterInput("jsonEncoded", NewJsonEncodedInputFactory())
	encoding.RegisterInput("jsonObject", NewJsonObjectInputFactory())

	encoding.RegisterOutput("jsonEncoded", NewJsonEncodedOutputFactory())
	encoding.RegisterOutput("jsonObject", NewJsonObjectOutputFactory())
}
