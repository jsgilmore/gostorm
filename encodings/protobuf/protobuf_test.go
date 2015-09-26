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
	"bytes"
	"fmt"
	proto "github.com/gogo/protobuf/gogoproto/gogo.proto"
	"github.com/jsgilmore/gostorm/messages"
	"math/rand"
	"testing"
)

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func newTestObj(name string, number int64, data []byte) *messages.Test {
	return &messages.Test{
		Name:   name,
		Number: number,
		Data:   data,
	}
}

func TestContSendRecv(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1<<20))
	//buffer := new(bytes.Buffer)
	output := NewProtobufOutput(buffer)

	var outMsgStream []*messages.Test
	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		outMsgStream = append(outMsgStream, outMsg)
		output.SendMsg(outMsg)
	}
	output.Flush()

	input := NewProtobufInput(buffer)
	for i := 0; i < 100; i++ {
		inMsg := &messages.Test{}
		err := input.ReadMsg(inMsg)
		checkErr(err, t)

		if !inMsg.Equal(outMsgStream[i]) {
			t.Fatalf("Written message (%v) does not equal read message (%v)", outMsgStream[i], inMsg)
		}
	}
}

func TestDiscSendRecv(t *testing.T) {
	buffer := new(bytes.Buffer)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		output.SendMsg(outMsg)
		output.Flush()

		inMsg := &messages.Test{}
		err := input.ReadMsg(inMsg)
		checkErr(err, t)

		if !inMsg.Equal(outMsg) {
			t.Fatalf("Written message (%v) does not equal read message (%v)", outMsg, inMsg)
		}
	}
}

func TestEmitGeneric(t *testing.T) {
	buffer := new(bytes.Buffer)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	needTaskIds := true

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))
		outMeta := &messages.ShellMsgMeta{
			Command:     "emit",
			Anchors:     []string{"1", "2"},
			Id:          &numStr,
			Stream:      &numStr,
			Task:        &num,
			NeedTaskIds: &needTaskIds,
			Msg:         &numStr,
		}
		output.EmitGeneric(outMeta.Command, outMeta.GetId(), outMeta.GetStream(), outMeta.GetMsg(), outMeta.GetAnchors(), outMeta.GetTask(), true, outMsg)
		output.Flush()

		shellMsg := &messages.ShellMsg{
			ShellMsgProto: &messages.ShellMsgProto{},
		}
		err := input.ReadMsg(shellMsg)
		checkErr(err, t)

		if !shellMsg.ShellMsgProto.ShellMsgMeta.Equal(outMeta) {
			t.Fatalf("Emission metadata (%+v) does not equal read emission metadata (%+v)", outMeta, shellMsg.ShellMsgProto.ShellMsgMeta)
		}

		inMsg := &messages.Test{}
		err = proto.Unmarshal(shellMsg.ShellMsgProto.Contents[0], inMsg)
		checkErr(err, t)
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Emission data (%+v) does not equal read emission data (%+v)", outMsg, inMsg)
		}
	}
}

func TestReadBoltMsg(t *testing.T) {
	buffer := new(bytes.Buffer)
	output := NewProtobufOutput(buffer)
	input := NewProtobufInput(buffer)

	for i := 0; i < 100; i++ {
		num := rand.Int63()
		numStr := fmt.Sprintf("%d", num)
		outMsg := newTestObj(numStr, num, []byte(numStr))

		outTuple := &messages.BoltMsg{
			BoltMsgProto: &messages.BoltMsgProto{
				BoltMsgMeta: &messages.BoltMsgMeta{
					Id:     numStr,
					Comp:   numStr,
					Stream: numStr,
					Task:   num,
				},
			},
		}
		outProto, err := proto.Marshal(outMsg)
		checkErr(err, t)
		outTuple.Contents = append(outTuple.Contents, outProto)
		output.SendMsg(outTuple)
		output.Flush()

		inMsg := &messages.Test{}
		inMeta := &messages.BoltMsgMeta{}
		err = input.ReadBoltMsg(inMeta, inMsg)
		checkErr(err, t)

		if !inMeta.Equal(outTuple.BoltMsgProto.BoltMsgMeta) {
			t.Fatalf("Tuple metadata (%+v) does not equal read Tuple metadata (%+v)", outTuple.BoltMsgProto.BoltMsgMeta, inMeta)
		}
		if !inMsg.Equal(outMsg) {
			t.Fatalf("Tuple data (%+v) does not equal read tuple data (%+v)", outMsg, inMsg)
		}
	}
}
