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

// GoStorm is a library that allows you to write Storm spouts and bolts in Go
package gostorm

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type mode int

const (
	spout mode = iota
	bolt
)

// BoltConn is the interface that implements the possible bolt actions
type BoltConn interface {
	Initialise(reader io.Reader, writer io.Writer)
	Context() *Context
	Log(msg string)
	ReadTuple(contentStructs ...interface{}) (meta *TupleMetadata, err error)
	ReadRawTuple() (tuple *TupleMsg, err error)
	SendAck(id string)
	SendFail(id string)
	Emit(anchors []string, stream string, content ...interface{}) (taskIds []int)
	EmitDirect(anchors []string, stream string, directTask int, contents ...interface{})
}

// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
	Initialise(reader io.Reader, writer io.Writer)
	Context() *Context
	Log(msg string)
	ReadMsg() (msg *spoutMsg, err error)
	SendSync()
	Emit(id string, stream string, contents ...interface{}) (taskIds []int)
	EmitDirect(id string, stream string, directTask int, contents ...interface{})
}

// newStormConn creates a new generic Storm connection
// This connection must be embedded in either a spout or bolt
func newStormConn(mode mode) *stormConnImpl {
	stormConn := &stormConnImpl{
		mode: mode,
	}
	return stormConn
}

// stormConnImpl represents the common functions that both a bolt and spout are capable of
type stormConnImpl struct {
	mode    mode
	reader  *bufio.Reader
	writer  io.Writer
	context *Context
}

func (this *stormConnImpl) readData() (data []byte, err error) {
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
func (this *stormConnImpl) readMsg(msg interface{}) error {
	data, err := this.readData()
	if err != nil {
		log.Printf("Invalid data: %s\n", data)
		return err
	}

	//log.Printf("Data: %s\n", data)
	err = json.Unmarshal(data, msg)
	if err != nil {
		return err
	}
	return nil
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *stormConnImpl) sendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(this.writer, string(data))
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
}

// sendMsg sends the contents of a known Storm message to Storm
func (this *stormConnImpl) sendEncoded(msg string) {
	fmt.Fprintln(this.writer, msg)
	// Storm requires that every message be suffixed with an "end" string
	fmt.Fprintln(this.writer, "end")
}

func (this *stormConnImpl) readContext() (context *Context, err error) {
	context = &Context{}
	err = this.readMsg(context)
	if err != nil {
		return nil, err
	}
	return context, nil
}

func (this *stormConnImpl) reportPid() {
	msg := pidMsg{
		Pid: os.Getpid(),
	}
	this.sendMsg(msg)

	pidDir := this.context.PidDir
	if len(pidDir) > 0 {
		pidDir += "/"
	}
	// Write an empty file with the pid, which storm can use to kill our process
	pidFile, err := os.Create(pidDir + strconv.Itoa(os.Getpid()))
	if err != nil {
		panic(err)
	}
	pidFile.Close()
}

// Initialise set the storm input reader to the specified file
// descriptor, reads the topology configuration for Storm and reports
// the pid to Storm
func (this *stormConnImpl) Initialise(reader io.Reader, writer io.Writer) {
	this.reader = bufio.NewReader(reader)
	this.writer = writer
	// Receive the topology config for this storm connection
	var err error
	this.context, err = this.readContext()
	if err != nil {
		panic(fmt.Sprintf("Storm failed to initialise: %v", err))
	}

	this.reportPid()
}

func (this *stormConnImpl) Context() *Context {
	return this.context
}

// Log sends a log message that will be logged by Storm
func (this *stormConnImpl) Log(text string) {
	msg := logMsg{
		Command: "log",
		Msg:     text,
	}
	this.sendMsg(msg)
}

//-------------------------------------------------------------------
// Bolt
//-------------------------------------------------------------------

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewBoltConn() BoltConn {
	boltConn := &boltConnImpl{
		stormConnImpl: newStormConn(bolt),
		tupleBuffer:   list.New(),
	}
	return boltConn
}

type boltConnImpl struct {
	*stormConnImpl
	tupleBuffer *list.List
}

func newTupleMetadata(id, comp, stream string, task int) *TupleMetadata {
	meta := &TupleMetadata{
		Id:     id,
		Comp:   comp,
		Stream: stream,
		Task:   task,
	}
	return meta
}

// readBytes reads data from stdin into the struct provided.
func (this *boltConnImpl) readMsg(msg interface{}) (err error) {
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
		return err
	}
	return nil
}

func (this *boltConnImpl) readTaskIds() (taskIds []int) {
	// Read a single json record from the input file
	data, err := this.readData()
	if err != nil {
		panic(err)
	}

	// If we didn't receive a json array, treat it as a tuple instead
	if data[0] != '[' {
		this.tupleBuffer.PushBack(data)
		return this.readTaskIds()
	}

	err = json.Unmarshal(data, &taskIds)
	if err != nil {
		panic(err)
	}
	return taskIds
}

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *boltConnImpl) ReadTuple(contentStructs ...interface{}) (metadata *TupleMetadata, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}

	tuple := &TupleMsg{}
	for _, contentStruct := range contentStructs {
		tuple.AddContent(contentStruct)
	}
	err = this.readMsg(tuple)
	if err != nil {
		return nil, err
	}

	return tuple.TupleMetadata, nil
}

// ReadRawTuple reads a tuple from Storm of which the contents are unknown.
// Json generic unmarshalling rules will apply to the contents
func (this *boltConnImpl) ReadRawTuple() (tuple *TupleMsg, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}

	tuple = &TupleMsg{}
	err = this.readMsg(tuple)
	if err != nil {
		return nil, err
	}
	return tuple, nil
}

// SendAck acks the received message id
// SendAck has to be called after an emission anchored to the acked id,
// otherwise Storm will report an error.
func (this *boltConnImpl) SendAck(id string) {
	this.sendEncoded(fmt.Sprintf(`{"command": "ack", "id": "%s"}`, id))
}

// SendFail reports that the message with the given Id failed
// No emission should be anchored to a failed message Id
func (this *boltConnImpl) SendFail(id string) {
	this.sendEncoded(fmt.Sprintf(`{"command": "fail", "id": "%s"}`, id))
}

func contentsAppend(contents ...interface{}) []interface{} {
	var contentList []interface{}
	for _, content := range contents {
		contentList = append(contentList, content)
	}
	return contentList
}

// Emit emits a tuple with the given array of interface{}s as values,
// anchored to the given array of taskIds, sent out on the given stream.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *boltConnImpl) Emit(anchors []string, stream string, contents ...interface{}) (taskIds []int) {
	emission := boltEmission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)
	return this.readTaskIds()
}

// EmitDirect emits a tuple with the given array of interface{}s as values,
// anchored to the given array of taskIds, sent out on the given stream,
// to the given taskId.
// The topology should have been configured for direct transmission
// for this call to work.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *boltConnImpl) EmitDirect(anchors []string, stream string, directTask int, contents ...interface{}) {
	emission := boltDirectEmission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Task:     directTask,
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)
}

//-------------------------------------------------------------------
// Spout
//-------------------------------------------------------------------

// NewSpoutConn returns a Storm spout connection that a Go spout can use to communicate with Storm
func NewSpoutConn() SpoutConn {
	spoutConn := &spoutConnImpl{
		stormConnImpl: newStormConn(spout),
	}
	return spoutConn
}

type spoutConnImpl struct {
	readyToSend bool
	*stormConnImpl
}

// ReadMsg reads a message from Storm.
// The message read can be either a next, ack or fail message.
// A check is performed to verify that Storm has been initialised.
func (this *spoutConnImpl) ReadMsg() (msg *spoutMsg, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}
	this.readyToSend = true

	msg = &spoutMsg{}
	err = this.readMsg(msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// SendSync sends a sync message to Storm.
// After a sync message is sent, it is not possible for a spout to
// emit a message before a ReadMsg has been performed. This is to
// enforce the synchronous behaviour of a spout as required by Storm.
func (this *spoutConnImpl) SendSync() {
	this.readyToSend = false
	this.sendEncoded(`{"command": "sync"}`)
}

// Emit emits a tuple with the given array of interface{}s as values,
// with the given taskId, sent out on the given stream.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *spoutConnImpl) Emit(id string, stream string, contents ...interface{}) (taskIds []int) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}

	emission := spoutEmission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)

	// Upon an indirect emit, storm replies with a list of chosen destination task Ids
	err := this.readMsg(&taskIds)
	// EOF is only an issue when using test files
	if err != nil {
		panic(err)
	}
	return taskIds
}

// EmitDirect emits a tuple with the given array of interface{}s as values,
// with the given taskId, sent out on the given stream, to the given taskId.
// The topology should have been configured for direct transmission
// for this call to work.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *spoutConnImpl) EmitDirect(id string, stream string, directTask int, contents ...interface{}) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}

	emission := spoutDirectEmission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Task:     directTask,
		Contents: contentsAppend(contents...),
	}
	this.sendMsg(emission)
}
