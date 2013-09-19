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

package protobuf

import (
	"bufio"
	"code.google.com/p/gogoprotobuf/proto"
	"container/list"
	"encoding/binary"
	"fmt"
	"github.com/jsgilmore/gostorm"
	"github.com/jsgilmore/gostorm/messages"
	"io"
	"log"
)

// Use BigEndian to make Java's life easier
var byteOrder = binary.BigEndian

func NewProtobufInputFactory() gostorm.InputFactory {
	return &protobufInputFactory{}
}

type protobufInputFactory struct{}

func (this *protobufInputFactory) NewInput(reader io.Reader) gostorm.Input {
	return NewProtobufInput(reader)
}

func NewProtobufInput(reader io.Reader) gostorm.Input {
	return &protobufInput{
		reader:      bufio.NewReader(reader),
		tupleBuffer: list.New(),
	}
}

type protobufInput struct {
	reader      *bufio.Reader
	tupleBuffer *list.List
}

func (this *protobufInput) readData() (data []byte, err error) {
	msgLen, err := binary.ReadUvarint(this.reader)
	if err != nil {
		return nil, err
	}
	data = make([]byte, msgLen)
	binary.Read(this.reader, byteOrder, data)
	return data, nil
}

// readBytes reads data from stdin into the struct provided.
func (this *protobufInput) ReadMsg(msg interface{}) (err error) {
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

	err = proto.Unmarshal(data, msg.(proto.Message))
	if err != nil {
		log.Printf("Gostorm protobuf encoding: Unmarshalling: %s", data)
		return err
	}
	return nil
}

func isTuple(data []byte) bool {
	return false
}

func (this *protobufInput) ReadTaskIds() (taskIds []int32) {
	// Read a single json record from the input file
	data, err := this.readData()
	if err != nil {
		panic(err)
	}

	// If we didn't receive a json array, treat it as a tuple instead
	if isTuple(data) {
		this.tupleBuffer.PushBack(data)
		return this.ReadTaskIds()
	}

	taskIdsProto := &messages.TaskIds{}
	err = proto.Unmarshal(data, taskIdsProto)
	if err != nil {
		panic(err)
	}
	return taskIdsProto.TaskIds
}

func (this *protobufInput) constructInput(contents ...interface{}) [][]byte {
	contentList := make([][]byte, len(contents))
	return contentList
}

func (this *protobufInput) decodeInput(contentList [][]byte, contentStructs ...interface{}) {
	for i, content := range contentStructs {
		err := proto.Unmarshal(contentList[i], content.(proto.Message))
		if err != nil {
			panic(err)
		}
	}
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *protobufInput) ReadTuple(contentStructs ...interface{}) (metadata *messages.TupleMetadata, err error) {
	tuple := &messages.TupleMsg{
		TupleProto: &messages.TupleProto{
			Contents: this.constructInput(contentStructs...),
		},
	}
	err = this.ReadMsg(tuple)
	if err != nil {
		return nil, err
	}

	this.decodeInput(tuple.Contents, contentStructs...)
	return tuple.TupleProto.TupleMetadata, nil
}

func NewProtobufOutputFactory() gostorm.OutputFactory {
	return &protobufOutputFactory{}
}

type protobufOutputFactory struct{}

func (this *protobufOutputFactory) NewOutput(writer io.Writer) gostorm.Output {
	return NewProtobufOutput(writer)
}

func NewProtobufOutput(writer io.Writer) gostorm.Output {
	return &protobufOutput{
		writer: writer,
	}
}

type protobufOutput struct {
	writer io.Writer
}

func varintSize(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

type ProtoMarshaler interface {
	Size() int
	MarshalTo(data []byte) (n int, err error)
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *protobufOutput) SendMsg(msg interface{}) {
	protoSiz := msg.(ProtoMarshaler).Size()
	varIntSiz := varintSize(uint64(protoSiz))
	buffer := make([]byte, varIntSiz+protoSiz)

	n := binary.PutUvarint(buffer, uint64(protoSiz))
	if varIntSiz != n {
		panic(fmt.Sprintf("Actual varint size did not match calculated varint size: %d instead of %d", n, varIntSiz))
	}

	n, err := msg.(ProtoMarshaler).MarshalTo(buffer[n:])
	if n+varIntSiz != len(buffer) {
		panic(fmt.Sprintf("Protobuf: Invalid size written by MarshalTo: %d instead of %d", n, len(buffer)))
	}
	if err != nil {
		panic(err)
	}

	err = binary.Write(this.writer, byteOrder, buffer)
	if err != nil {
		panic(err)
	}
}

func (this *protobufOutput) constructOutput(contents ...interface{}) [][]byte {
	contentList := make([][]byte, len(contents))
	for i, content := range contents {
		encoded, err := proto.Marshal(content.(proto.Message))
		if err != nil {
			panic(err)
		}
		contentList[i] = encoded
	}
	return contentList
}

func (this *protobufOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, contents ...interface{}) {
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
			Contents: this.constructOutput(contents...),
		},
	}
	this.SendMsg(emission)
}

func init() {
	gostorm.RegisterInput("protobuf", NewProtobufInputFactory())
	gostorm.RegisterOutput("protobuf", NewProtobufOutputFactory())
}
