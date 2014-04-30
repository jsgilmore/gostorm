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
	"github.com/jsgilmore/gostorm"
	stormmsg "github.com/jsgilmore/gostorm/messages"
)

func NewPrinter() gostorm.Bolt {
	return &printer{}
}

type printer struct{}

func (this *printer) Execute(meta stormmsg.BoltMsgMeta, fields ...interface{}) {
	for _, field := range fields {
		fmt.Printf("%v\n", field)
	}
}

func (this *printer) Prepare(context *stormmsg.Context, collector gostorm.OutputCollector) {

}

func (this *printer) Cleanup() {

}

func (this *printer) Fields() []interface{} {
	return nil
}

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewMockOutputCollector(bolt gostorm.Bolt) gostorm.OutputCollector {
	outputCollector := &mockOutputCollectorImpl{
		bolt: bolt,
	}
	return outputCollector
}

type mockOutputCollectorImpl struct {
	bolt gostorm.Bolt
}

func (this *mockOutputCollectorImpl) Log(msg string) {
}

func (this *mockOutputCollectorImpl) SendAck(id string) {
	this.EmitDirect(nil, "", 0, "Ack:"+id)
}

func (this *mockOutputCollectorImpl) SendFail(id string) {
	this.EmitDirect(nil, "", 0, "Fail:"+id)
}

func (this *mockOutputCollectorImpl) Emit(anchors []string, stream string, contents ...interface{}) (taskIds []int32) {
	this.EmitDirect(anchors, stream, 0, contents...)
	return []int32{1}
}

func (this *mockOutputCollectorImpl) EmitDirect(anchors []string, stream string, directTask int64, contents ...interface{}) {
	meta := stormmsg.BoltMsgMeta{
		Stream: stream,
	}
	this.bolt.Execute(meta, contents...)
}

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewMockSpoutOutputCollector(bolt gostorm.Bolt) gostorm.SpoutOutputCollector {
	spoutOutputCollector := &mockSpoutSpoutOutputCollectorImpl{
		bolt: bolt,
	}
	return spoutOutputCollector
}

type mockSpoutSpoutOutputCollectorImpl struct {
	bolt gostorm.Bolt
}

func (this *mockSpoutSpoutOutputCollectorImpl) Log(msg string) {
}

func (this *mockSpoutSpoutOutputCollectorImpl) Emit(id string, stream string, contents ...interface{}) (taskIds []int32) {
	this.EmitDirect(id, stream, 0, contents...)
	return []int32{1}
}

func (this *mockSpoutSpoutOutputCollectorImpl) EmitDirect(id string, stream string, directTask int64, contents ...interface{}) {
	meta := stormmsg.BoltMsgMeta{
		Id:     id,
		Stream: stream,
	}
	this.bolt.Execute(meta, contents...)
}
