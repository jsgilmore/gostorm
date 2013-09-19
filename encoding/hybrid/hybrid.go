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

package hybrid

import (
	"bufio"
	"bytes"
	"code.google.com/p/gogoprotobuf/proto"
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/jsgilmore/gostorm"
	"github.com/jsgilmore/gostorm/messages"
	"io"
	"log"
)

func NewHybridInputFactory() gostorm.InputFactory {
	return &hybridInputFactory{}
}

type hybridInputFactory struct{}

func (this *hybridInputFactory) NewInput(reader io.Reader) gostorm.Input {
	return NewHybridInput(reader)
}

func NewHybridInput(reader io.Reader) gostorm.Input {
	return &hybridInput{
		reader:      bufio.NewReader(reader),
		tupleBuffer: list.New(),
	}
}

type hybridInput struct {
	reader      *bufio.Reader
	tupleBuffer *list.List
}

func (this *hybridInput) readData() (data []byte, err error) {
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
func (this *hybridInput) ReadMsg(msg interface{}) (err error) {
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
		log.Printf("Gostorm hybrid encoding: Unmarshalling: %s", data)
		return err
	}
	return nil
}

func (this *hybridInput) ReadTaskIds() (taskIds []int32) {
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

func (this *hybridInput) constructInput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i := 0; i < len(contents); i++ {
		contentList[i] = &[]byte{}
	}
	return contentList
}

func (this *hybridInput) decodeInput(contentList []interface{}, contentStructs ...interface{}) {
	for i, content := range contentStructs {
		err := proto.Unmarshal(*contentList[i].(*[]byte), content.(proto.Message))
		if err != nil {
			panic(err)
		}
	}
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *hybridInput) ReadTuple(contentStructs ...interface{}) (metadata *messages.TupleMetadata, err error) {
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

func NewHybridOutputFactory() gostorm.OutputFactory {
	return &hybridOutputFactory{}
}

type hybridOutputFactory struct{}

func (this *hybridOutputFactory) NewOutput(writer io.Writer) gostorm.Output {
	return NewHybridOutput(writer)
}

func NewHybridOutput(writer io.Writer) gostorm.Output {
	return &hybridOutput{
		writer: writer,
	}
}

type hybridOutput struct {
	writer io.Writer
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *hybridOutput) SendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(this.writer, string(data))
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
}

func (this *hybridOutput) constructOutput(contents ...interface{}) []interface{} {
	contentList := make([]interface{}, len(contents))
	for i, content := range contents {
		encoded, err := proto.Marshal(content.(proto.Message))
		if err != nil {
			panic(err)
		}
		contentList[i] = &encoded
	}
	return contentList
}

func (this *hybridOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{}) {
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
	gostorm.RegisterInput("hybrid", NewHybridInputFactory())
	gostorm.RegisterOutput("hybrid", NewHybridOutputFactory())
}
