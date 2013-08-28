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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewMockBoltConn(reader io.Reader, writer io.Writer) BoltConn {
	boltConn := &mockBoltConnImpl{
		reader: bufio.NewReader(reader),
		writer: writer,
	}
	return boltConn
}

type mockBoltConnImpl struct {
	reader *bufio.Reader
	writer io.Writer
}

func (this *mockBoltConnImpl) Context() *Context {
	return nil
}

func (this *mockBoltConnImpl) sendMsg(msg interface{}) {
	fmt.Fprintf(this.writer, "%v\n", msg)
}

func (this *mockBoltConnImpl) Log(text string) {
	this.sendMsg(text)
}

func (this *mockBoltConnImpl) Initialise() {
	// A mock boltConn does not have a context
}

func (this *mockBoltConnImpl) ReadTuple(contentStructs ...interface{}) (metadata *TupleMetadata, err error) {
	return nil, nil
}

func (this *mockBoltConnImpl) SendAck(id string) {
	this.sendMsg("Ack:" + id)
}

func (this *mockBoltConnImpl) SendFail(id string) {
	this.sendMsg("Fail:" + id)
}

func (this *mockBoltConnImpl) Emit(anchors []string, stream string, contents ...interface{}) (taskIds []int) {
	for _, content := range contents {
		this.sendMsg(content)
	}
	return nil
}

func (this *mockBoltConnImpl) EmitDirect(anchors []string, stream string, directTask int, contents ...interface{}) {
	for _, content := range contents {
		this.sendMsg(content)
	}
}

// NewBoltConn returns a Storm bolt connection that a Go bolt can use to communicate with Storm
func NewMockSpoutConn(json bool, writer io.Writer) SpoutConn {
	spoutConn := &mockSpoutConnImpl{
		json:   json,
		writer: writer,
	}
	return spoutConn
}

type mockSpoutConnImpl struct {
	json   bool
	writer io.Writer
}

func (this *mockSpoutConnImpl) Context() *Context {
	return nil
}

func (this *mockSpoutConnImpl) sendMsg(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(this.writer, string(data))
}

func (this *mockSpoutConnImpl) sendEncoded(msg interface{}) {
	fmt.Fprintf(this.writer, "%v\n", msg)
}

func (this *mockSpoutConnImpl) Log(text string) {
	this.sendEncoded(text)
}

func (this *mockSpoutConnImpl) Initialise() {
	// A mock boltConn does not have a context
}

func (this *mockSpoutConnImpl) ReadSpoutMsg() (msg *spoutMsg, err error) {
	return &spoutMsg{
		Command: "next",
	}, nil
}

func (this *mockSpoutConnImpl) SendSync() {
	this.sendEncoded("sync")
}

func (this *mockSpoutConnImpl) Emit(id string, stream string, contents ...interface{}) (taskIds []int) {
	for _, content := range contents {
		if this.json {
			this.sendMsg(content)
		} else {
			this.sendEncoded(content)
		}
	}
	return nil
}

func (this *mockSpoutConnImpl) EmitDirect(id string, stream string, directTask int, contents ...interface{}) {
	for _, content := range contents {
		if this.json {
			this.sendMsg(content)
		} else {
			this.sendEncoded(content)
		}
	}
}
