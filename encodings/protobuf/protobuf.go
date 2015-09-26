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
	"container/list"
	"encoding/binary"
	"fmt"
	proto "github.com/jsgilmore/gostorm/Godeps/_workspace/src/github.com/gogo/protobuf/proto"
	"github.com/jsgilmore/gostorm/core"
	"github.com/jsgilmore/gostorm/messages"
	"io"
)

func NewProtobufInputFactory() core.InputFactory {
	return &protobufInputFactory{}
}

type protobufInputFactory struct{}

func (this *protobufInputFactory) NewInput(reader io.Reader) core.Input {
	return NewProtobufInput(reader)
}

func NewProtobufInput(reader io.Reader) core.Input {
	return &protobufInput{
		// TODO Only a spout should have an unbuffered byte reader
		reader:      bufio.NewReader(reader),
		tupleBuffer: list.New(),
		bufferPool:  NewBufferPoolSingle(NewAllocatorHeap()),
	}
}

type protobufInput struct {
	reader      *bufio.Reader
	tupleBuffer *list.List
	bufferPool  BufferPool
}

func (this *protobufInput) readData() (data []byte, err error) {
	msgLen, err := binary.ReadUvarint(this.reader)
	if err != nil {
		return nil, err
	}
	data = this.bufferPool.New(int(msgLen))
	// ReadFull is required since a bufio reader can return less data
	// than required in a single read
	_, err = io.ReadFull(this.reader, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// readBytes reads data from stdin into the struct provided.
func (this *protobufInput) ReadMsg(msg interface{}) (err error) {
	var data []byte
	var bufferPoolRead bool
	if this.tupleBuffer.Len() > 0 {
		// Read data from the tuple buffer
		e := this.tupleBuffer.Front()
		data = this.tupleBuffer.Remove(e).([]byte)
		bufferPoolRead = false
	} else {
		// if the tuple buffer is empty, read data from storm
		data, err = this.readData()
		if err != nil {
			return err
		}
		bufferPoolRead = true
	}

	err = proto.Unmarshal(data, msg.(proto.Message))
	if bufferPoolRead {
		// If the buffer pool was mmapped, we don't want to mix heap and mmapped data
		this.bufferPool.Dispose(data)
	}
	return err
}

// TODO This asynchronous behaviour should still be added to protobuf
func isTuple(data []byte) bool {
	return false
}

func (this *protobufInput) ReadTaskIds() (taskIds []int32) {
	// Read a single json record from the input file
	data, err := this.readData()
	if err != nil {
		panic(err)
	}

	if isTuple(data) {
		// Since we want to dispose of the data slice to reuse it for
		// reading, we create a new slice for buffering. Creating a
		// new slice here is ok, since we probably didn't create one
		// when we actually read the data.
		bufferedData := make([]byte, len(data))
		copy(bufferedData, data)
		this.tupleBuffer.PushBack(bufferedData)
		return this.ReadTaskIds()
	}

	taskIdsProto := &messages.TaskIds{}
	err = proto.Unmarshal(data, taskIdsProto)
	if err != nil {
		panic(err)
	}
	this.bufferPool.Dispose(data)
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
func (this *protobufInput) ReadBoltMsg(metadata *messages.BoltMsgMeta, contentStructs ...interface{}) (err error) {
	boltMsg := &messages.BoltMsg{
		BoltMsgProto: &messages.BoltMsgProto{
			//BoltMsgMeta: metadata,
			Contents: this.constructInput(contentStructs...),
		},
	}
	err = this.ReadMsg(boltMsg)
	if err != nil {
		return err
	}

	this.decodeInput(boltMsg.Contents, contentStructs...)
	// TODO This assignment needs to be replaced with the top
	// assignmet and unmarshalMerge used when that feature has been
	// added to gogoprotobuf
	*metadata = *boltMsg.BoltMsgMeta
	return nil
}

func NewProtobufOutputFactory() core.OutputFactory {
	return &protobufOutputFactory{}
}

type protobufOutputFactory struct{}

func (this *protobufOutputFactory) NewOutput(writer io.Writer) core.Output {
	return NewProtobufOutput(writer)
}

func NewProtobufOutput(writer io.Writer) core.Output {
	shellMsg := &messages.ShellMsg{
		ShellMsgProto: &messages.ShellMsgProto{
			ShellMsgMeta: &messages.ShellMsgMeta{},
		},
	}

	return &protobufOutput{
		writer:     bufio.NewWriter(writer),
		bufferPool: NewBufferPoolSingle(NewAllocatorHeap()),
		shellMsg:   shellMsg,
	}
}

type protobufOutput struct {
	writer     *bufio.Writer
	bufferPool BufferPool
	shellMsg   *messages.ShellMsg
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
	protoMsg := msg.(ProtoMarshaler)
	protoSiz := protoMsg.Size()
	varIntSiz := varintSize(uint64(protoSiz))
	buffer := this.bufferPool.New(varIntSiz + protoSiz)

	n := binary.PutUvarint(buffer, uint64(protoSiz))
	if varIntSiz != n {
		panic(fmt.Sprintf("Actual varint size did not match calculated varint size: %d instead of %d", n, varIntSiz))
	}

	n, err := protoMsg.MarshalTo(buffer[n:])
	if n+varIntSiz != len(buffer) {
		panic(fmt.Sprintf("Protobuf: Invalid size written by MarshalTo: %d instead of %d", n, len(buffer)))
	}
	if err != nil {
		panic(err)
	}

	n, err = this.writer.Write(buffer)
	if n != varIntSiz+protoSiz {
		panic("Incorrect length written")
	}
	if err != nil {
		panic(err)
	}
	this.bufferPool.Dispose(buffer)
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

func (this *protobufOutput) EmitGeneric(command, id, stream, msg string, anchors []string, directTask int64, needTaskIds bool, contents ...interface{}) {
	meta := this.shellMsg.ShellMsgMeta
	meta.Command = command
	meta.Anchors = anchors
	meta.Id = &id
	meta.Stream = &stream
	meta.Task = &directTask
	meta.NeedTaskIds = &needTaskIds
	meta.Msg = &msg
	this.shellMsg.ShellMsgProto.Contents = this.constructOutput(contents...)
	this.SendMsg(this.shellMsg)
}

func (this *protobufOutput) Flush() {
	this.writer.Flush()
}

func init() {
	core.RegisterInput("protobuf", NewProtobufInputFactory())
	core.RegisterOutput("protobuf", NewProtobufOutputFactory())
}
