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
	"github.com/jsgilmore/gostorm"
	"github.com/jsgilmore/gostorm/messages"
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
		log.Printf("Gostorm json: Unmarshalling: %s", data)
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

func NewJsonObjectInputFactory() gostorm.InputFactory {
	return &jsonObjectInputFactory{}
}

type jsonObjectInputFactory struct{}

func (this *jsonObjectInputFactory) NewInput(reader io.Reader) gostorm.Input {
	return NewJsonObjectInput(reader)
}

func NewJsonObjectInput(reader io.Reader) gostorm.Input {
	return &jsonObjectInput{
		jsonInput: newJsonInput(reader),
	}
}

type jsonObjectInput struct {
	*jsonInput
}

// ConstructInput for object json returns a list of objects into which json objects should be unmarshalled
func (this *jsonObjectInput) constructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		contentList[i] = content
	}
	return contentList
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *jsonObjectInput) ReadTuple(contentStructs ...interface{}) (metadata *messages.TupleMetadata, err error) {
	tuple := &messages.TupleMsg{
		TupleJson: &messages.TupleJson{
			Contents: this.constructInput(contentStructs...),
		},
	}

	err = this.ReadMsg(tuple)
	if err != nil {
		return nil, err
	}

	return tuple.TupleJson.TupleMetadata, nil
}

func NewJsonEncodedInputFactory() gostorm.InputFactory {
	return &jsonEncodedInputFactory{}
}

type jsonEncodedInputFactory struct{}

func (this *jsonEncodedInputFactory) NewInput(reader io.Reader) gostorm.Input {
	return NewJsonEncodedInput(reader)
}

func NewJsonEncodedInput(reader io.Reader) gostorm.Input {
	return &jsonEncodedInput{
		jsonInput: newJsonInput(reader),
	}
}

type jsonEncodedInput struct {
	*jsonInput
}

// ConstructInput for encoded json returns a list of bytes into which marshalled json should be written
func (this *jsonEncodedInput) constructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i := 0; i < len(contents); i++ {
		contentList[i] = &[]byte{}
	}
	return contentList
}

func (this *jsonEncodedInput) decodeInput(contentList []interface{}, contentStructs ...interface{}) {
	for i, content := range contentStructs {
		err := json.Unmarshal(*contentList[i].(*[]byte), content)
		if err != nil {
			panic(err)
		}
	}
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *jsonEncodedInput) ReadTuple(contentStructs ...interface{}) (metadata *messages.TupleMetadata, err error) {
	tuple := &messages.TupleMsg{
		TupleJson: &messages.TupleJson{
			Contents: this.constructInput(contentStructs...),
		},
	}
	err = this.ReadMsg(tuple)
	if err != nil {
		return nil, err
	}

	this.decodeInput(tuple.TupleJson.Contents, contentStructs...)
	return tuple.TupleJson.TupleMetadata, nil
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
func (this *jsonOutput) SendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(this.writer, string(data))
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
}

func NewJsonEncodedOutputFactory() gostorm.OutputFactory {
	return &jsonEncodedOutputFactory{}
}

type jsonEncodedOutputFactory struct{}

func (this *jsonEncodedOutputFactory) NewOutput(writer io.Writer) gostorm.Output {
	return NewJsonEncodedOutput(writer)
}

func NewJsonEncodedOutput(writer io.Writer) gostorm.Output {
	return &jsonEncodedOutput{
		jsonOutput: newJsonOutput(writer),
	}
}

type jsonEncodedOutput struct {
	*jsonOutput
}

func (this *jsonEncodedOutput) constructOutput(contents ...interface{}) []interface{} {
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

func (this *jsonEncodedOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{}) {
	emission := &messages.Emission{
		EmissionProto: &messages.EmissionProto{
			EmissionMetadata: &messages.EmissionMetadata{
				Command: command,
				Anchors: anchors,
				Id:      &id,
				Stream:  &stream,
				Task:    &directTask,
				Msg:     &msg,
			},
		},
		ContentsJson: this.constructOutput(contents...),
	}
	this.SendMsg(emission)
}

func NewJsonObjectOutputFactory() gostorm.OutputFactory {
	return &jsonObjectOutputFactory{}
}

type jsonObjectOutputFactory struct{}

func (this *jsonObjectOutputFactory) NewOutput(writer io.Writer) gostorm.Output {
	return NewJsonObjectOutput(writer)
}

func NewJsonObjectOutput(writer io.Writer) gostorm.Output {
	return &jsonObjectOutput{
		jsonOutput: newJsonOutput(writer),
	}
}

type jsonObjectOutput struct {
	*jsonOutput
}

func (this *jsonObjectOutput) constructOutput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		contentList[i] = content
	}
	return contentList
}

func (this *jsonObjectOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{}) {
	emission := &messages.Emission{
		EmissionProto: &messages.EmissionProto{
			EmissionMetadata: &messages.EmissionMetadata{
				Command: command,
				Anchors: anchors,
				Id:      &id,
				Stream:  &stream,
				Task:    &directTask,
				Msg:     &msg,
			},
		},
		ContentsJson: this.constructOutput(contents...),
	}
	this.SendMsg(emission)
}

func init() {
	gostorm.RegisterInput("jsonEncoded", NewJsonEncodedInputFactory())
	gostorm.RegisterInput("jsonObject", NewJsonObjectInputFactory())

	gostorm.RegisterOutput("jsonEncoded", NewJsonEncodedOutputFactory())
	gostorm.RegisterOutput("jsonObject", NewJsonObjectOutputFactory())
}
