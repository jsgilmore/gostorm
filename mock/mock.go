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

package gostorm

import (
	"fmt"
	gostorm "github.com/jsgilmore/gostorm/core"
	"github.com/jsgilmore/gostorm/messages"
	"log"
)

type Consumer interface {
	Event(msgs ...interface{})
}

type Runner interface {
	Run()
}

func NewPrinter() Consumer {
	return &printer{}
}

type printer struct{}

func (this *printer) Event(msgs ...interface{}) {
	for _, msg := range msgs {
		fmt.Printf("%v\n", msg)
	}
}

type context struct{}

type tupleMetaData struct{}

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewMockBoltConn(consumer Consumer) gostorm.BoltConn {
	boltConn := &mockBoltConnImpl{
		consumer: consumer,
	}
	return boltConn
}

type mockBoltConnImpl struct {
	consumer Consumer
}

func (this *mockBoltConnImpl) Context() *messages.Context {
	return nil
}

func (this *mockBoltConnImpl) sendMsg(msg interface{}) {
	this.consumer.Event(msg)
}

func (this *mockBoltConnImpl) Log(text string) {
	log.Println(text)
}

func (this *mockBoltConnImpl) Connect() {
	// A mock boltConn does not have a context
}

func (this *mockBoltConnImpl) ReadTuple(contentStructs ...interface{}) (meta *messages.TupleMetadata, err error) {
	return nil, nil
}

func (this *mockBoltConnImpl) SendAck(id string) {
	this.sendMsg("Ack:" + id)
}

func (this *mockBoltConnImpl) SendFail(id string) {
	this.sendMsg("Fail:" + id)
}

func (this *mockBoltConnImpl) Emit(anchors []string, stream string, contents ...interface{}) (taskIds []int32) {
	this.consumer.Event(contents...)
	return []int32{1}
}

func (this *mockBoltConnImpl) EmitDirect(anchors []string, stream string, directTask int64, contents ...interface{}) {
	this.consumer.Event(contents...)
}

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewMockSpoutConn(consumer Consumer) gostorm.SpoutConn {
	spoutConn := &mockSpoutConnImpl{
		consumer: consumer,
	}
	return spoutConn
}

type mockSpoutConnImpl struct {
	consumer Consumer
}

func (this *mockSpoutConnImpl) Context() *messages.Context {
	return nil
}

func (this *mockSpoutConnImpl) Log(text string) {
	this.consumer.Event(text)
}

func (this *mockSpoutConnImpl) Connect() {
	// A mock boltConn does not have a context
}

func (this *mockSpoutConnImpl) ReadSpoutMsg() (command, id string, err error) {
	return "next", "", nil
}

func (this *mockSpoutConnImpl) SendSync() {}

func (this *mockSpoutConnImpl) Emit(id string, stream string, contents ...interface{}) (taskIds []int32) {
	this.consumer.Event(contents...)
	return []int32{1}
}

func (this *mockSpoutConnImpl) EmitDirect(id string, stream string, directTask int64, contents ...interface{}) {
	this.consumer.Event(contents...)
}
