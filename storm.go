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
	"errors"
	"fmt"
	"github.com/jsgilmore/gostorm/encoding"
	"os"
	"strconv"
)

// BoltConn is the interface that implements the possible bolt actions
type BoltConn interface {
	Initialise()
	Context() *Context
	Log(msg string)
	ReadTuple(contentStructs ...interface{}) (meta *TupleMetadata, err error)
	SendAck(id string)
	SendFail(id string)
	Emit(anchors []string, stream string, content ...interface{}) (taskIds []int)
	EmitDirect(anchors []string, stream string, directTask int, contents ...interface{})
}

// SpoutConn is the interface that implements the possible spout actions
type SpoutConn interface {
	Initialise()
	Context() *Context
	Log(msg string)
	ReadSpoutMsg() (msg *spoutMsg, err error)
	SendSync()
	Emit(id string, stream string, contents ...interface{}) (taskIds []int)
	EmitDirect(id string, stream string, directTask int, contents ...interface{})
}

// newStormConn creates a new generic Storm connection
// This connection must be embedded in either a spout or bolt
func newStormConn(in encoding.Input, out encoding.Output) *stormConnImpl {
	stormConn := &stormConnImpl{
		Input:  in,
		Output: out,
	}
	return stormConn
}

// stormConnImpl represents the common functions that both a bolt and spout are capable of
type stormConnImpl struct {
	encoding.Input
	encoding.Output
	context *Context
}

func (this *stormConnImpl) readContext() (context *Context, err error) {
	context = &Context{}
	err = this.ReadMsg(context)
	if err != nil {
		return nil, err
	}
	return context, nil
}

func (this *stormConnImpl) reportPid() {
	msg := pidMsg{
		Pid: os.Getpid(),
	}
	this.SendMsg(msg)

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
func (this *stormConnImpl) Initialise() {
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
	this.SendMsg(msg)
}

//-------------------------------------------------------------------
// Bolt
//-------------------------------------------------------------------

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewBoltConn(in encoding.Input, out encoding.Output) BoltConn {
	boltConn := &boltConnImpl{
		stormConnImpl: newStormConn(in, out),
	}
	return boltConn
}

type boltConnImpl struct {
	*stormConnImpl
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

// ReadTuple reads a tuple from Storm of which the contents are known
// and decodes the contents into the provided list of structs
func (this *boltConnImpl) ReadTuple(contentStructs ...interface{}) (metadata *TupleMetadata, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}

	tuple := &TupleMsg{
		Contents: this.ConstructInput(contentStructs...),
	}
	err = this.ReadMsg(tuple)
	if err != nil {
		return nil, err
	}

	this.DecodeInput(tuple.Contents, contentStructs...)
	return tuple.TupleMetadata, nil
}

// SendAck acks the received message id
// SendAck has to be called after an emission anchored to the acked id,
// otherwise Storm will report an error.
func (this *boltConnImpl) SendAck(id string) {
	this.SendEncoded(fmt.Sprintf(`{"command": "ack", "id": "%s"}`, id))
}

// SendFail reports that the message with the given Id failed
// No emission should be anchored to a failed message Id
func (this *boltConnImpl) SendFail(id string) {
	this.SendEncoded(fmt.Sprintf(`{"command": "fail", "id": "%s"}`, id))
}

// Emit emits a tuple with the given array of interface{}s as values,
// anchored to the given array of taskIds, sent out on the given stream.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *boltConnImpl) Emit(anchors []string, stream string, contents ...interface{}) (taskIds []int) {
	msg := emission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Contents: this.ConstructOutput(contents...),
	}
	this.SendMsg(msg)
	return this.ReadTaskIds()
}

// EmitDirect emits a tuple with the given array of interface{}s as values,
// anchored to the given array of taskIds, sent out on the given stream,
// to the given taskId.
// The topology should have been configured for direct transmission
// for this call to work.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *boltConnImpl) EmitDirect(anchors []string, stream string, directTask int, contents ...interface{}) {
	msg := emission{
		Command:  "emit",
		Anchors:  anchors,
		Stream:   stream,
		Task:     directTask,
		Contents: this.ConstructOutput(contents...),
	}
	this.SendMsg(msg)
}

//-------------------------------------------------------------------
// Spout
//-------------------------------------------------------------------

// NewSpoutConn returns a Storm spout connection that a Go spout can use to communicate with Storm
func NewSpoutConn(in encoding.Input, out encoding.Output) SpoutConn {
	spoutConn := &spoutConnImpl{
		stormConnImpl: newStormConn(in, out),
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
func (this *spoutConnImpl) ReadSpoutMsg() (msg *spoutMsg, err error) {
	if this.context == nil {
		return nil, errors.New("Attempting to read from uninitialised Storm connection")
	}
	this.readyToSend = true

	msg = &spoutMsg{}
	err = this.ReadMsg(msg)
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
	this.SendEncoded(`{"command": "sync"}`)
}

// Emit emits a tuple with the given array of interface{}s as values,
// with the given taskId, sent out on the given stream.
// A stream value of "" or "default" can be used to denote the default stream
// The function returns a list of taskIds to which the message was sent.
func (this *spoutConnImpl) Emit(id string, stream string, contents ...interface{}) (taskIds []int) {
	if !this.readyToSend {
		panic("Spout not ready to send")
	}

	msg := emission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Contents: this.ConstructOutput(contents...),
	}
	this.SendMsg(msg)
	return this.ReadTaskIds()
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

	msg := emission{
		Command:  "emit",
		Id:       id,
		Stream:   stream,
		Task:     directTask,
		Contents: this.ConstructOutput(contents...),
	}
	this.SendMsg(msg)
}
